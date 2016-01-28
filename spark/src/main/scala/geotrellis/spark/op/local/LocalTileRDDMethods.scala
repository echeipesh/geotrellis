package geotrellis.spark.op.local

import geotrellis.raster.op.local._
import geotrellis.spark.op._
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.Partitioner

trait LocalTileRDDMethods[K] extends TileRDDMethods[K]
    with AddTileRDDMethods[K]
    with AndTileRDDMethods[K]
    with IfCellTileRDDMethods[K]
    with DivideTileRDDMethods[K]
    with EqualTileRDDMethods[K]
    with GreaterOrEqualTileRDDMethods[K]
    with GreaterTileRDDMethods[K]
    with LessOrEqualTileRDDMethods[K]
    with LessTileRDDMethods[K]
    with LocalMapTileRDDMethods[K]
    with MajorityTileRDDMethods[K]
    with MaxTileRDDMethods[K]
    with MinTileRDDMethods[K]
    with MinorityTileRDDMethods[K]
    with MultiplyTileRDDMethods[K]
    with OrTileRDDMethods[K]
    with PowTileRDDMethods[K]
    with SubtractTileRDDMethods[K]
    with UnequalTileRDDMethods[K]
    with XorTileRDDMethods[K] {

  /**
    * Generate a raster with the values from the first raster, but only include
    * cells in which the corresponding cell in the second raster *are not* set to the
    * "readMask" value.
    *
    * For example, if *all* cells in the second raster are set to the readMask value,
    * the output raster will be empty -- all values set to NODATA.
    */
  def localMask(other: Self, readMask: Int, writeMask: Int): Self = localMask(other, readMask, writeMask, None)
  def localMask(other: Self, readMask: Int, writeMask: Int, partitioner: Option[Partitioner]): Self =
    self.combineValues(other, partitioner) {
      case (r1, r2) => Mask(r1, r2, readMask, writeMask)
    }

  /**
    * Generate a raster with the values from the first raster, but only include
    * cells in which the corresponding cell in the second raster is set to the
    * "readMask" value.
    *
    * For example, if *all* cells in the second raster are set to the readMask value,
    * the output raster will be identical to the first raster.
    */
  def localInverseMask(other: Self, readMask: Int, writeMask: Int): Self =
    localInverseMask(other, readMask, writeMask, None)
  def localInverseMask(other: Self, readMask: Int, writeMask: Int, partitioner: Option[Partitioner]): Self =
    self.combineValues(other, partitioner) {
      case (r1, r2) => InverseMask(r1, r2, readMask, writeMask)
    }

  /** Maps an integer typed Tile to 1 if the cell value is not NODATA, otherwise 0. */
  def localDefined() =
    self.mapValues { r => Defined(r) }

  /** Maps an integer typed Tile to 1 if the cell value is NODATA, otherwise 0. */
  def localUndefined() =
    self.mapValues { r => Undefined(r) }

  /** Take the square root each value in a raster. */
  def localSqrt() =
    self.mapValues { r => Sqrt(r) }

  /** Round the values of a Tile. */
  def localRound() =
    self.mapValues { r => Round(r) }

  /** Computes the Log of Tile values. */
  def localLog() =
    self.mapValues { r => Log(r) }

  /** Computes the Log base 10 of Tile values. */
  def localLog10() =
    self.mapValues { r => Log10(r) }

  /** Takes the Flooring of each raster cell value. */
  def localFloor() =
    self.mapValues { r => Floor(r) }

  /** Takes the Ceiling of each raster cell value. */
  def localCeil() =
    self.mapValues { r => Ceil(r) }

  /**
    * Negate (multiply by -1) each value in a raster.
    */
  def localNegate() =
    self.mapValues { r => Negate(r) }

  /** Negate (multiply by -1) each value in a raster. */
  def unary_-() = localNegate()

  /**
    * Bitwise negation of Tile.
    *
    * @note               NotRaster does not currently support Double raster data.
    *                     If you use a Tile with a Double CellType (TypeFloat, TypeDouble)
    *                     the data values will be rounded to integers.
    */
  def localNot() =
    self.mapValues { r => Not(r) }

  /** Takes the Absolute value of each raster cell value. */
  def localAbs() =
    self.mapValues { r => Abs(r) }

  /**
    * Takes the arc cos of each raster cell value.
    *
    * @info               Always return a double valued raster.
    */
  def localAcos() =
    self.mapValues { r => Acos(r) }

  /**
    * Takes the arc sine of each raster cell value.
    *
    * @info               Always return a double valued raster.
    */
  def localAsin() =
    self.mapValues { r => Asin(r) }

  /** Takes the Arc Tangent2
    *  This raster holds the y - values, and the parameter
    *  holds the x values. The arctan is calculated from y / x.
    *
    *  @info               A double raster is always returned.
    */
  def localAtan2(other: Self): Self = localAtan2(other, None)
  def localAtan2(other: Self, partitioner: Option[Partitioner]): Self =
    self.combineValues(other, partitioner)(Atan2.apply)

  /**
    * Takes the arc tan of each raster cell value.
    *
    * @info               Always return a double valued raster.
    */
  def localAtan() =
    self.mapValues { r => Atan(r) }

  /** Takes the Cosine of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localCos() =
    self.mapValues { r => Cos(r) }

  /** Takes the hyperbolic cosine of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localCosh() =
    self.mapValues { r => Cosh(r) }

  /**
    * Takes the sine of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localSin() =
    self.mapValues { r => Sin(r) }

  /**
    * Takes the hyperbolic sine of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localSinh() =
    self.mapValues { r => Sinh(r) }

  /** Takes the Tangent of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localTan() =
    self.mapValues { r => Tan(r) }

  /** Takes the hyperboic cosine of each raster cell value.
    *
    * @info               Always returns a double raster.
    */
  def localTanh() =
    self.mapValues { r => Tanh(r) }
}
