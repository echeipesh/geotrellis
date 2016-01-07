package geotrellis.spark.etl.hadoop

import geotrellis.raster.Tile
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import geotrellis.spark.io.hadoop._
import org.apache.spark.rdd.RDD

class GeoTiffHadoopInput extends HadoopInput[ProjectedExtent, Tile] {
  val format = "geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(props("path"))
}

