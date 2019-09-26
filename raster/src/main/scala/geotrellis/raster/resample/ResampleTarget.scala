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

package geotrellis.raster.resample

import geotrellis.raster.{RasterExtent, GridExtent, GridBounds}
import geotrellis.raster.CellSize
import geotrellis.vector.Extent

import spire.math.Integral
import spire.implicits._

/** Represents a strategy/target for resampling */
sealed trait ResampleTarget[N] {
  /** Provided a gridextent, construct a new [[GridExtent]] that satisfies target constraint(s) */
  def apply(source: => GridExtent[N]): GridExtent[N]
}

/** Resample, aiming for a specific number of cell columns/rows */
case class TargetDimensions[N: Integral](cols: N, rows: N) extends ResampleTarget[N] {
  def apply(source: => GridExtent[N]): GridExtent[N] =
    new GridExtent(source.extent, cols, rows)
}

/** Snap to a target grid - useful prior to comparison between rasters
 * as a means of ensuring clear correspondence between underlying cell values
 */
case class TargetGrid[N: Integral](grid: GridExtent[Long]) extends ResampleTarget[N] {
  def apply(source: => GridExtent[N]): GridExtent[N] =
    grid.createAlignedGridExtent(source.extent).toGridType[N]
}

/** Resample, sampling values into a user-supplied [[GridExtent]] */
case class TargetGridExtent[N: Integral](gridExtent: GridExtent[N]) extends ResampleTarget[N] {
  def apply(source: => GridExtent[N]): GridExtent[N] =
    gridExtent
}

/** Resample, aiming for a grid which has the provided [[CellSize]]
 *
 * @note Targetting a specific size for each cell in the grid has consequences for the
 * [[Extent]] because e.g. an extent's width *must* be evenly divisible by the width of
 * the cells within it. Consequently, we have two options: either modify the resolution
 * to accomodate the output extent or modify the overall extent to preserve the desired
 * output resolution. Fine grained constraints on both resolution and extent will currently
 * need to be managed manually.
 */
case class TargetCellSize[N: Integral](cellSize: CellSize) extends ResampleTarget[N] {
  // the logic in this method comes from RasterExtentReproject it avoids
  // issues related to remainder cellsize when the extent isn't evenly divided up
  // by slightly altering the output extent to accomodate the desired cell size
  // TODO: we should look into using this logic in `GridExtent.withResolution`
  def apply(source: => GridExtent[N]): GridExtent[N] = {
    val newCols = (source.extent.width / cellSize.width + 0.5).toLong
    val newRows = (source.extent.height / cellSize.height + 0.5).toLong

    //Adjust the extent to match the pixel size.
    val adjustedExtent =
      Extent(source.extent.xmin, source.extent.ymax - (cellSize.height * newRows), source.extent.xmin + (cellSize.width * newCols), source.extent.ymax)

    new GridExtent[N](adjustedExtent, cellSize)
  }
}

/** Resample, targetting the exact boundary encoded in the provided [[GridBounds]] */
case class TargetGridBounds[N: Integral](bounds: GridBounds[N]) extends ResampleTarget[N] {
  def apply(source: => GridExtent[N]): GridExtent[N] =
    source.createAlignedGridExtent(source.extentFor(bounds, clamp=false))
}
