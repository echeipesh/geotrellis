package geotrellis.spark.etl.hadoop

import geotrellis.raster.MultiBandTile
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import geotrellis.spark.io.hadoop._
import org.apache.spark.rdd.RDD

class MultibandGeoTiffHadoopInput extends HadoopInput[ProjectedExtent, MultiBandTile] {
  val format = "multiband-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultiBandTile)] =
    sc.hadoopMultiBandGeoTiffRDD(props("path"))
}

