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
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path
import geotrellis.spark.io.hadoop.HdfsUtils
import org.apache.accumulo.core.util.CachedConfiguration
import org.apache.accumulo.core.conf.{AccumuloConfiguration, Property}

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
    val rdd = sc.newAPIHadoopRDD(job.getConfiguration, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
    decode(rdd, metaData)
  }

  def loadTile(accumulo: AccumuloInstance)(id: LayerId, metaData: RasterMetaData, table: String, key: K): Tile

  def accumuloIngestDir: Path = {
    val conf = AccumuloConfiguration.getSiteConfiguration
    new Path(conf.get(Property.INSTANCE_DFS_DIR), "ingest")
  }

  /** NOTE: Accumulo will always perform destructive update, clobber param is not followed */
  def save(sc: SparkContext, accumulo: AccumuloInstance)(id: LayerId, raster: RasterRDD[K], table: String, clobber: Boolean): Unit = {
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
        .map { case (key, tile) => getKey(id, key) -> new Value(tile.toBytes) }        
        .sortByKey(ascending = true)
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
