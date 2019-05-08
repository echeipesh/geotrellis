/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.summary.polygonal.visitors

import geotrellis.raster._
import geotrellis.raster.summary.CellVisitor

abstract class MultibandTileFoldingVisitor
    extends CellVisitor[Raster[MultibandTile], Array[Option[Double]]] {

  private var accumulator = Array[Option[Double]]()

  def result: Array[Option[Double]] = accumulator

  def visit(raster: Raster[MultibandTile], col: Int, row: Int): Unit = {
    val tiles = raster.tile.bands.toArray
    val values: Array[Option[Double]] = result ++ Array.fill[Option[Double]](
      tiles.size - result.size)(None)
    accumulator = tiles.zip(values).map {
      case (tile: Tile, maybeAccum: Option[Double]) =>
        val newValue = tile.getDouble(col, row)
        maybeAccum match {
          case Some(accum) if isData(newValue) => Some(fold(accum, newValue))
          case None if isData(newValue)        => Some(newValue)
          case _                               => maybeAccum
        }
    }
  }

  def fold(accum: Double, newValue: Double): Double
}
