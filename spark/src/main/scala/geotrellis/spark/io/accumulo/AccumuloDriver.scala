package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.raster._
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mapreduce.{ AccumuloFileOutputFormat, AccumuloOutputFormat, AccumuloInputFormat, InputFormatBase }
import org.apache.accumulo.core.data.{ Value, Key, Mutation }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path
import geotrellis.spark.io.hadoop.HdfsUtils
import org.apache.accumulo.core.util.CachedConfiguration
import org.apache.accumulo.core.conf.{AccumuloConfiguration, Property}
import geotrellis.raster.stats.FastMapHistogram
import java.util.concurrent.TimeUnit

class TableNotFoundError(table: String) extends Exception(s"Target Accumulo table `$table` does not exist.")

trait AccumuloDriver[K] extends Serializable {
  def encode(layerId: LayerId, raster: RasterRDD[K]): RDD[(Text, Mutation)]
  def decode(rdd: RDD[(Key, Value)], metaData: RasterMetaData): RasterRDD[K]
  def setFilters(job: Job, layerId: LayerId, filters: FilterSet[K])
  def rowId(id: LayerId, key: K): String
  def getKey(id: LayerId, key: K): Key
    
  def load(sc: SparkContext, accumulo: AccumuloInstance)(id: LayerId, metaData: RasterMetaData, table: String, filters: FilterSet[K]): RasterRDD[K] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    accumulo.setAccumuloConfig(job)
    InputFormatBase.setInputTableName(job, table)
    setFilters(job, id, filters)
    val rdd = sc.newAPIHadoopRDD(job.getConfiguration, classOf[BatchAccumuloInputFormat], classOf[Key], classOf[Value])
    decode(rdd, metaData)
  }

  def loadTile(accumulo: AccumuloInstance)(id: LayerId, metaData: RasterMetaData, table: String, key: K): Tile

  def accumuloIngestDir: Path = {
    val conf = AccumuloConfiguration.getSiteConfiguration
    new Path(conf.get(Property.INSTANCE_DFS_DIR), "ingest")
  }


  def saveWithIf(sc: SparkContext, accumulo: AccumuloInstance)(id: LayerId, raster: RasterRDD[K], table: String, clobber: Boolean): Unit = {
    val connector = accumulo.connector    
    val ops = connector.tableOperations()  
    if (! ops.exists(table))  ops.create(table)
    
    val groups = ops.getLocalityGroups(table)
    val newGroup: java.util.Set[Text] = Set(new Text(id.name))
    ops.setLocalityGroups(table, groups.updated(table, newGroup))
    
    val job = Job.getInstance(sc.hadoopConfiguration)
    val conf = job.getConfiguration    

    val splits = getSplits(id, raster)
    ops.addSplits(table, new java.util.TreeSet(splits.map(new Text(_))))

    val bcCon = sc.broadcast(connector)
    
    val mutations = 
    raster.mapPartitions{ pairs =>
      val partitionHist = FastMapHistogram() 
      var rows = Map.empty[String, List[(Key, Tile)]]

      pairs.foreach { case (key, tile) =>
        partitionHist.update(FastMapHistogram.fromTile(tile))

        val rid = rowId(id, key)        
        val list = rows.getOrElse(rid, Nil)
        val rowKey = getKey(id, key)
        rows = rows.updated(rid, rowKey -> tile :: list)
      }
      
      println(partitionHist)

      rows.map { case (rid, list) => 
        val mut = new Mutation(rid)
        list.foreach { case (rowKey, tile) =>
          mut.put(rowKey.getColumnFamily, rowKey.getColumnQualifier, 
            System.currentTimeMillis(), new Value(tile.toBytes))
        }
        (null, mut)
      }.toIterator
    }
    
    accumulo.setAccumuloConfig(job)
    AccumuloOutputFormat.setBatchWriterOptions(job, 
      new BatchWriterConfig()
        .setMaxMemory(4*1024*1024) 
        .setMaxWriteThreads(24)
        .setMaxLatency(5, TimeUnit.SECONDS))
    AccumuloOutputFormat.setDefaultTableName(job, table) 

    mutations.saveAsNewAPIHadoopFile(accumulo.instanceName, classOf[Text], classOf[Mutation], classOf[AccumuloOutputFormat], job.getConfiguration)
  }

  def save(sc: SparkContext, accumulo: AccumuloInstance)(id: LayerId, raster: RasterRDD[K], table: String, clobber: Boolean): Unit = {
    val connector = accumulo.connector    
    val ops = connector.tableOperations()  
    if (! ops.exists(table))  ops.create(table)
    
    val groups = ops.getLocalityGroups(table)
    val newGroup: java.util.Set[Text] = Set(new Text(id.name))
    ops.setLocalityGroups(table, groups.updated(table, newGroup))
    
    val job = Job.getInstance(sc.hadoopConfiguration)
    val conf = job.getConfiguration    

    val splits = getSplits(id, raster)
    ops.addSplits(table, new java.util.TreeSet(splits.map(new Text(_))))

    val bcCon = sc.broadcast(connector)
    
    val rddHist = 
    raster.mapPartitions{ pairs =>
      val partitionHist = FastMapHistogram() 
      var rows = Map.empty[String, List[(Key, Tile)]]

      pairs.foreach { case (key, tile) =>
        partitionHist.update(FastMapHistogram.fromTile(tile))

        val rid = rowId(id, key)        
        val list = rows.getOrElse(rid, Nil)
        val rowKey = getKey(id, key)
        rows = rows.updated(rid, rowKey -> tile :: list)
      }

      val writer = bcCon.value.createBatchWriter(table, 
        new BatchWriterConfig()
          .setMaxMemory(4*1024*1024) 
          .setMaxWriteThreads(24)
          .setMaxLatency(5, TimeUnit.SECONDS))
  
      //We've just taken a lot of memory converting all the tiles to bites
      // there does not seem to be an expidient way to avoid it    
      var counter = 0
      rows.foreach { case (rid, list) => 
        val mut = new Mutation(rid)
        list.foreach { case (rowKey, tile) =>
          mut.put(rowKey.getColumnFamily, rowKey.getColumnQualifier, 
            System.currentTimeMillis(), new Value(tile.toBytes))
        }
        writer.addMutation(mut)
        
        //Lets try to clear the pipes a little and stave off overflows
        counter += 1
        counter %= 100
        if (counter == 0) writer.flush()
      }      
      writer.close()
      Iterator(partitionHist)
    }
    .collect
    .foldLeft(FastMapHistogram()){ (acc, h) => acc.update(h); acc }
    // We actually have nothing to do with the histogram here, lets dump it for our viewign pleasure
    println(rddHist)
    rddHist
  }

  /** NOTE: Accumulo will always perform destructive update, clobber param is not followed */
  def saveAsFile(sc: SparkContext, accumulo: AccumuloInstance)(id: LayerId, raster: RasterRDD[K], table: String, clobber: Boolean): Unit = {
    val connector = accumulo.connector    
    val ops = connector.tableOperations()  
    if (! ops.exists(table))  ops.create(table)
    
    val groups = ops.getLocalityGroups(table)
    val newGroup: java.util.Set[Text] = Set(new Text(id.name))
    ops.setLocalityGroups(table, groups.updated(table, newGroup))
    
    val job = Job.getInstance(sc.hadoopConfiguration)
    val conf = job.getConfiguration
    
    val outPath = HdfsUtils.tmpPath(accumuloIngestDir, s"${id.name}-${id.zoom}", conf)        
    val failuresPath = outPath.suffix("-failures")
    
    try {
      raster                
        .sortBy{ case (key, _) => getKey(id, key) }
        .map { case (key, tile) => getKey(id, key) -> new Value(tile.toBytes) }        
        .saveAsNewAPIHadoopFile(outPath.toString, classOf[Key], classOf[Value], classOf[AccumuloFileOutputFormat], job.getConfiguration)

      HdfsUtils.ensurePathExists(failuresPath, conf)

      ops.importDirectory(table, outPath.toString, failuresPath.toString, true)
    } 
    finally {
      HdfsUtils.deletePath(outPath, conf)
      HdfsUtils.deletePath(failuresPath, conf)
    }
  }

 def getSplits(id: LayerId, rdd: RasterRDD[K], num: Int = 24): Seq[String] = {
    import org.apache.spark.SparkContext._

    rdd
      .map { case (key, _) => rowId(id, key) -> null }
      .sortByKey(ascending = true, numPartitions = num)
      .map(_._1)
      .mapPartitions{ iter => iter.take(1) }
      .collect
  }
}
