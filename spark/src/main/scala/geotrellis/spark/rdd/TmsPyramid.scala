package geotrellis.spark.rdd

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobID, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.tiling._
import geotrellis.spark.TmsTile

import scalaz.Memo._

object TmsPyramid {
  def fromPath(path: String)(implicit sc: SparkContext): TmsPyramid = 
    new TmsPyramid(new Path(path))

  final val SeqFileGlob = "/*[0-9]*/data"
}

case class BufferRDD(rdd: RDD[TmsTile], extent: TileExtent)

class TmsPyramid(path: Path)(implicit sc: SparkContext) {
  val meta = PyramidMetadata(path, sc.hadoopConfiguration)

  val layerHadoopConfig: (Int => Configuration) = immutableHashMapMemo{ zoom => 
    /**
     * This is going to result in one hadoop configuration for each layer.
     * Each configuration is going to contain a Job and Paths to the data files
     */
    val layerPath = new Path(path, s"$zoom")
    val job = new Job(sc.hadoopConfiguration)
    val globbedPath = new Path(layerPath.toUri().toString()  + TmsPyramid.SeqFileGlob)
    FileInputFormat.addInputPath(job, globbedPath)
    job.getConfiguration
  }

  def rdd(zoom: Int): RasterRDD = ???

  def rdd(zoom: Int, extent: TileExtent): RasterRDD = {
    implicit val hadoopConfig = layerHadoopConfig(zoom)
    CroppedRasterHadoopRDD(new Path(path, s"$zoom"), extent, zoom, meta).toRasterRDD(false) 
  }

  def getBuffer(zoom: Int, x: Int, y: Int, pad: Int): BufferRDD = {
    val extent = TileExtent(x - pad, y - pad, x + pad, y + pad)
    BufferRDD(rdd(zoom, extent).persist, extent)  
  }
}