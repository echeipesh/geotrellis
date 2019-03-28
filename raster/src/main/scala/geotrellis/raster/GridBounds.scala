/*
 * Copyright 2016 Azavea
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

package geotrellis.raster

import scala.collection.mutable
import spire.syntax.cfor._
import spire.math._
import spire.implicits._

/**
  * Represents grid coordinates of a subsection of a RasterExtent.
  * These coordinates are inclusive.
  */
case class GridBounds[@specialized(Int, Long) N: Integral](colMin: N, rowMin: N, colMax: N, rowMax: N) {
  def width: N = colMax - colMin + 1
  def height: N = rowMax - rowMin + 1
  def size: Long = width.toLong * height.toLong
  def isEmpty: Boolean = size == 0

  /**
    * Return true if the present [[GridBounds]] contains the position
    * pointed to by the given column and row, otherwise false.
    *
    * @param  col  The column
    * @param  row  The row
    */
  def contains(col: N, row: N): Boolean =
    (colMin <= col && col <= colMax) && (rowMin <= row && row <= rowMax)

  /**
    * Returns true if the present [[GridBounds]] and the given one
    * intersect (including their boundaries), otherwise returns false.
    *
    * @param  other  The other GridBounds
    */
  def intersects(other: GridBounds[N]): Boolean =
    !(colMax < other.colMin || other.colMax < colMin) &&
    !(rowMax < other.rowMin || other.rowMax < rowMin)

  /**
   * Creates a new [[GridBounds]] using a buffer around this
   * GridBounds.
   *
   * @note This will not buffer past 0 regardless of how much the buffer
   *       falls below it.
   *
   * @param bufferSize The amount this GridBounds should be buffered by.
   */
  def buffer(bufferSize: Int): GridBounds[N] =
    buffer(bufferSize, bufferSize)

  /**
   * Creates a new [[GridBounds]] using a buffer around this
   * GridBounds.
   *
   * @note This will not buffer past 0 regardless of how much the buffer
   *       falls below it.
   *
   * @param colBuffer The amount the cols within this GridBounds should be buffered.
   * @param rowBuffer The amount the rows within this GridBounds should be buffered.
   * @param clamp     Determines whether or not to clamp the GridBounds to the grid
   *                  such that it no value will be under 0; defaults to true. If false,
   *                  then the resulting GridBounds can contain negative values outside
   *                  of the grid boundaries.
   */
  def buffer(colBuffer: N, rowBuffer: N, clamp: Boolean = true): GridBounds[N] =
    GridBounds(
      if (clamp) (colMin - colBuffer).max(0) else colMin - colBuffer,
      if (clamp) (rowMin - rowBuffer).max(0) else rowMin - rowBuffer,
      colMax + colBuffer,
      rowMax + rowBuffer
    )

  /**
   * Offsets this [[GridBounds]] to a new location relative to its current
   * position
   *
   * @param boundsOffset The amount the GridBounds should be shifted.
   */
  def offset(boundsOffset: N): GridBounds[N] =
    offset(boundsOffset, boundsOffset)

  /**
   * Offsets this [[GridBounds]] to a new location relative to its current
   * position
   *
   * @param colOffset The amount the cols should be shifted.
   * @param rowOffset The amount the rows should be shifted.
   */
  def offset(colOffset: N, rowOffset: N): GridBounds[N] =
    GridBounds(colMin + colOffset, rowMin + rowOffset, colMax + colOffset, rowMax + rowOffset)

  /**
    * Another name for the 'minus' method.
    *
    * @param  other  The other GridBounds
    */
  def -(other: GridBounds[N]): Seq[GridBounds[N]] = minus(other)

  /**
    * Returns the difference of the present [[GridBounds]] and the
    * given one.  This returns a sequence, because the difference may
    * consist of more than one GridBounds.
    *
    * @param  other  The other GridBounds
    */
  def minus(other: GridBounds[N]): Seq[GridBounds[N]] =
    if(!intersects(other)) {
      Seq(this)
    } else {
      val overlapColMin =
        if(colMin < other.colMin) other.colMin
        else colMin

      val overlapColMax =
        if(colMax < other.colMax) colMax
        else other.colMax

      val overlapRowMin =
        if(rowMin < other.rowMin) other.rowMin
        else rowMin

      val overlapRowMax =
        if(rowMax < other.rowMax) rowMax
        else other.rowMax

      val result = mutable.ListBuffer[GridBounds[N]]()
      // Left cut
      if(colMin < overlapColMin) {
        result += GridBounds(colMin, rowMin, overlapColMin - 1, rowMax)
      }

      // Right cut
      if(overlapColMax < colMax) {
        result += GridBounds(overlapColMax + 1, rowMin, colMax, rowMax)
      }

      // Top cut
      if(rowMin < overlapRowMin) {
        result += GridBounds(overlapColMin, rowMin, overlapColMax, overlapRowMin - 1)
      }

      // Bottom cut
      if(overlapRowMax < rowMax) {
        result += GridBounds(overlapColMin, overlapRowMax + 1, overlapColMax, rowMax)
      }
      result
    }

  // /**
  //   * Return the coordinates covered by the present [[GridBounds]].
  //   */
  // def coordsIter: Iterator[(Int, Int)] = for {
  //   row <- Iterator.range(0, height)
  //   col <- Iterator.range(0, width)
  // } yield (col + colMin, row + rowMin)

  // /**
  //   * Return the intersection of the present [[GridBounds]] and the
  //   * given [[CellGrid]].
  //   *
  //   * @param  cellGrid  The cellGrid to intersect with
  //   */
  // def intersection(cellGrid: CellGrid): Option[GridBounds[N]] =
  //   intersection(GridBounds(cellGrid))

  /**
    * Return the intersection of the present [[GridBounds]] and the
    * given [[GridBounds]].
    *
    * @param  other  The other GridBounds
    */
  def intersection(other: GridBounds[N]): Option[GridBounds[N]] =
    if(!intersects(other)) {
      None
    } else {
      Some(
        GridBounds(
          colMin max other.colMin,
          rowMin max other.rowMin,
          colMax min other.colMax,
          rowMax min other.rowMax)
      )
    }

  /** Return the union of GridBounds. */
  def combine(other: GridBounds[N]): GridBounds[N] =
    GridBounds(
      colMin = this.colMin min other.colMin,
      rowMin = this.rowMin min other.rowMin,
      colMax = this.colMax max other.colMax,
      rowMax = this.rowMax max other.rowMax)

  /** Empty gridbounds contain nothing, though non empty gridbounds contains iteslf */
  def contains(other: GridBounds[N]): Boolean =
    if(colMin == 0 && colMax == 0 && rowMin == 0 && rowMax == 0) false
    else
      other.colMin >= colMin &&
      other.rowMin >= rowMin &&
      other.colMax <= colMax &&
      other.rowMax <= rowMax

  /** Split into windows, covering original CellBounds */
  def split(cols: N, rows: N)(implicit ev: NumberTag[N]): Iterator[CellBounds[N]] = {
    for {
      windowRowMin <- Interval.closed(rowMin, rowMax).iterator(rows)
      windowColMin <- Interval.closed(colMin, colMax).iterator(cols)
    } yield {
      CellBounds(
        colMin = windowColMin,
        rowMin = windowRowMin,
        colMax = Integral[N].min(windowColMin + cols - 1, colMax),
        rowMax = Integral[N].min(windowRowMin + rows - 1, rowMax)
      )
    }
  }
}


/**
  * The companion object for the [[GridBounds]] type.
  */
  object GridBounds {
    /**
      * Given a [[CellGrid]], produce the corresponding [[GridBounds]].
      *
      * @param  r  The given CellGrid
      */
    def apply(r: CellGrid): GridBounds[Int] =
      GridBounds(0, 0, r.cols-1, r.rows-1)

    /**
      * Given a sequence of keys, return a [[GridBounds]] of minimal
      * size which covers them.
      *
      * @param  keys  The sequence of keys to cover
      */
    // def envelope(keys: Iterable[Product2[Int, Int]]): GridBounds = {
    //   var colMin = Integer.MAX_VALUE
    //   var colMax = Integer.MIN_VALUE
    //   var rowMin = Integer.MAX_VALUE
    //   var rowMax = Integer.MIN_VALUE

    //   for (key <- keys) {
    //     val col = key._1
    //     val row = key._2
    //     if (col < colMin) colMin = col
    //     if (col > colMax) colMax = col
    //     if (row < rowMin) rowMin = row
    //     if (row > rowMax) rowMax = row
    //   }
    //   GridBounds(colMin, rowMin, colMax, rowMax)
    // }
    // TODO: put this on TileBounds

    /**
      * Creates a sequence of distinct [[GridBounds]] out of a set of
      * potentially overlapping grid bounds.
      *
      * @param  gridBounds  A traversable collection of GridBounds
      */
    def distinct[N](gridBounds: Traversable[GridBounds[N]]): Seq[GridBounds[N]] =
      gridBounds.foldLeft(Seq[GridBounds[N]]()) { (acc, bounds) =>
        acc ++ acc.foldLeft(Seq(bounds)) { (cuts, bounds) =>
          cuts.flatMap(_ - bounds)
        }
      }
  }