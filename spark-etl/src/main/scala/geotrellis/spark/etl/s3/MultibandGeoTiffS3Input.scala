package geotrellis.spark.etl.s3

import geotrellis.raster.{MultiBandTile, Tile}
import geotrellis.spark.ingest._
import geotrellis.spark.io.s3.{MultibandGeoTiffS3InputFormat, GeoTiffS3InputFormat}
import geotrellis.spark.SpatialKey
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MultibandGeoTiffS3Input extends S3Input[ProjectedExtent, MultiBandTile] {
  val format = "multiband-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultiBandTile)] =
    sc.newAPIHadoopRDD(
      configuration(props), classOf[MultibandGeoTiffS3InputFormat], classOf[ProjectedExtent], classOf[MultiBandTile]
    )
}