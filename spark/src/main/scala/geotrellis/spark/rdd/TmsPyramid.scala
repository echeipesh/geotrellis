package geotrellis.spark.rdd

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobID, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.SparkContext

import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.tiling.TileExtent

import scalaz.Memo._

object TmsPyramid {
  def fromPath(path: String)(implicit sc: SparkContext): TmsPyramid = 
    new TmsPyramid(new Path(path))
}

// TODO: refactor to include TmsLevel which is going to be paramed on zoom

class TmsPyramid(path: Path)(implicit sc: SparkContext) {
  // TODO? maybe this just needs to be fully merged into this class?
  val meta = PyramidMetadata(path, sc.hadoopConfiguration)
  
  final val SeqFileGlob = "/*[0-9]*/data"

  val layerHadoopConfig: (Int => Configuration) = immutableHashMapMemo{ zoom => 
    val layerPath = new Path(path, s"$zoom")
    println(layerPath.toString())
    val job = new Job(sc.hadoopConfiguration)
    val globbedPath = new Path(layerPath.toUri().toString()  + SeqFileGlob)
    FileInputFormat.addInputPath(job, globbedPath)
    job.getConfiguration
  }

  /**
   * Get a RasterRDD for the entire level of the pyramid
   */
  def rdd(zoom: Int): RasterRDD = ???

  /**
   * Get a RasterRDD that is pre-filtered to the given extent.
   * Those partitions not covered by the extent will not be fetched.
   */
  def rdd(zoom: Int, extent: TileExtent): RasterRDD = {
    implicit val hadoopConfig = layerHadoopConfig(zoom)
    CroppedRasterHadoopRDD(new Path(path, s"$zoom"), extent, zoom, meta).toRasterRDD(false)
    
  }

  def tile(zoom: Int, x: Int, y: Int) = ???
}