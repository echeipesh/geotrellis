package geotrellis.spark.op.local

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.op._
import geotrellis.raster.op.local.LessOrEqual
import org.apache.spark.Partitioner

trait LessOrEqualTileRDDMethods[K] extends TileRDDMethods[K] {
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def localLessOrEqual(i: Int) =
    self.mapValues { r => LessOrEqual(r, i) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def localLessOrEqualRightAssociative(i: Int) =
    self.mapValues { r => LessOrEqual(i, r) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def <=(i: Int) = localLessOrEqual(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def <=:(i: Int) = localLessOrEqualRightAssociative(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def localLessOrEqual(d: Double) =
    self.mapValues { r => LessOrEqual(r, d) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def localLessOrEqualRightAssociative(d: Double) =
    self.mapValues { r => LessOrEqual(d, r) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def <=(d: Double) = localLessOrEqual(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def <=:(d: Double) = localLessOrEqualRightAssociative(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than or equal to the
    * next raster, else 0.
    */
  def localLessOrEqual(other: Self): Self = localLessOrEqual(other, None)
  def localLessOrEqual(other: Self, partitioner: Option[Partitioner]): Self =
    self.combineValues(other, partitioner)(LessOrEqual.apply)

  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than or equal to the
    * next raster, else 0.
    */
  def <=(other: Self): Self = <=(other, None)
  def <=(other: Self, partitioner: Option[Partitioner]): Self = localLessOrEqual(other, partitioner)
}
