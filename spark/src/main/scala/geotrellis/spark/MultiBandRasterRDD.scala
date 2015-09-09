package geotrellis.spark

import geotrellis.raster.{Tile, MultiBandTile}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MultiBandRasterRDD[K: ClassTag](
  val tileRdd: RDD[(K, MultiBandTile)],
  bounds: KeyBounds[K],
  val metaData: RasterMetaData)
extends BoundRDD[K, MultiBandTile](tileRdd, bounds) {
  override val partitioner = tileRdd.partitioner

}

object MultiBandRasterRDD {
  def apply[K: ClassTag](rdd: RDD[(K, MultiBandTile)], bounds: KeyBounds[K], metaData: RasterMetaData) =
    new MultiBandRasterRDD[K](rdd, bounds, metaData)

  def apply[K: ClassTag](boundRdd: BoundRDD[K, MultiBandTile], metaData: RasterMetaData) =
    new MultiBandRasterRDD[K](boundRdd, boundRdd.bounds, metaData)
}
