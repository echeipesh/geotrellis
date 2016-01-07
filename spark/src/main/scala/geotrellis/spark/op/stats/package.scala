package geotrellis.spark.op

import geotrellis.raster.Tile
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

package object stats {
  implicit class withStatsTileRDDMethods[K: ClassTag](val self: RDD[(K, Tile)])
    extends StatsTileRDDMethods[K] { }
}
