package geotrellis.spark.rdd

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext

import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.tiling.TileExtent

object TmsPyramid {
  def fromPath(path: String)(implicit sc: SparkContext): TmsPyramid = 
    new TmsPyramid(new Path(path))
}

// TODO: refactor to include TmsLevel which is going to be paramed on zoom

class TmsPyramid(path: Path)(implicit sc: SparkContext) {
  // TODO? maybe this just needs to be fully merged into this class?
  val meta = PyramidMetadata(path, sc.hadoopConfiguration)
  
  /**
   * Get a RasterRDD for the entire level of the pyramid
   */
  def rdd(zoom: Int): RasterRDD = ???

  /**
   * Get a RasterRDD that is pre-filtered to the given extent.
   * Those partitions not covered by the extent will not be fetched.
   */
  def rdd(zoom: Int, extent: TileExtent): RasterRDD = {
    CroppedRasterHadoopRDD(new Path(path, s"$zoom"), extent, zoom, meta).toRasterRDD(false)
    
  }

  def tile(zoom: Int, x: Int, y: Int) = ???
}