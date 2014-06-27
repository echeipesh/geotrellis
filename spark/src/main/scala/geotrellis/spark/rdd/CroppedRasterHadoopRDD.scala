package geotrellis.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.spark._
import geotrellis.spark.formats._
import geotrellis.spark.metadata.Context
import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.tiling.{TmsTiling, TileExtent}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

import org.apache.spark.{InterruptibleIterator, Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.hadoop.conf.{Configurable, Configuration}



//case class TileExent(ll: Long, ur: Long)
/*
* An RDD abstraction of rasters in Spark. This can give back either tuples of either
* (TileIdWritable, ArgWritable) or (Long, Raster), the latter being the deserialized
* form of the former. See companion object
*/
class CroppedRasterHadoopRDD private (
    raster: Path, extent: TileExtent, 
    zoom: Int, meta: PyramidMetadata,
    sc: SparkContext, conf: Configuration)
  extends FilteredHadoopRDD[TileIdWritable, ArgWritable](
    sc,
    classOf[SequenceFileInputFormat[TileIdWritable, ArgWritable]],
    classOf[TileIdWritable],
    classOf[ArgWritable],
    conf) {

  /*
   * Overriding the partitioner with a TileIdPartitioner
   */
  override val partitioner = Some(TileIdPartitioner(raster, conf))

  /**
   * returns true if specific partition has TileIDs for extent
   */
  override
  def includePartition(p: Partition): Boolean = {
    //test if partition range intersects with a set of row ranges
    def intersects(rows: Seq[(Long, Long)], range: (Long, Long)): Boolean = {
      for (row <- rows) {
        if ( //If the row edges are in range or row fully includes the range
          (row._1 >= range._1 && row._1 <= range._2) ||
            (row._2 >= range._1 && row._2 <= range._2) ||
            (row._1 < range._1 && row._2 > range._2)
        ) return true
      }
      false
    }

    val range = partitioner.get.range(p.index)
    intersects(extent.getRowRanges(zoom), (range._1.get, range._2.get))
  }

  /**
   * returns true if the specific TileID is in the extent
   */
  override
  def includeKey(key: TileIdWritable): Boolean = extent.contains(zoom)(key.get)

  def toRasterRDD(addUserNoData: Boolean = false): RasterRDD =
    mapPartitions { partition =>
      partition.map { writableTile =>        
        writableTile.toTmsTile(meta, zoom, addUserNoData)
      }
    }
    .withContext(Context(zoom, meta, partitioner.get)) // .get is safe because it can't be 'None'
}

object CroppedRasterHadoopRDD {
  final val SeqFileGlob = "/*[0-9]*/data"

  def apply(raster: Path, extent: TileExtent, zoom: Int, meta:  PyramidMetadata)
      (implicit sc: SparkContext, hc: Configuration): CroppedRasterHadoopRDD = {
    //we expect that hc was updated to include the files needed and Job
    new CroppedRasterHadoopRDD(raster, extent, zoom, meta, sc, hc)
  }

}
