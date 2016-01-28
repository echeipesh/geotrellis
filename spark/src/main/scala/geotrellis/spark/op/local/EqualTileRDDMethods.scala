package geotrellis.spark.op.local

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.op._
import geotrellis.raster.op.local.Equal
import org.apache.spark.Partitioner

trait EqualTileRDDMethods[K] extends TileRDDMethods[K] {
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is equal to the input
    * integer, else 0.
    */
  def localEqual(i: Int) =
    self.mapValues { r => Equal(r, i) }

  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is equal to the input
    * double, else 0.
    */
  def localEqual(d: Double) =
    self.mapValues { r => Equal(r, d) }

  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is equal to the provided
    * raster, else 0.
    */
  def localEqual(other: Self, partitioner: Option[Partitioner] = None): Self =
    self.combineValues(other, partitioner)(Equal.apply)
}
