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

package geotrellis.raster.summary

/** Visitor for cell values of T that should record their state of R
  *
  * The user should implement concrete subclasses that update the value of `result` as
  * necessary on each call to `visit(raster: T, col: Int, row: Int)`.
  *
  * Be sure to handle the empty state. This could occur if no points in T are ever visited.
  */
trait CellVisitor[-T, R] {
  def result: R

  def visit(raster: T, col: Int, row: Int): Unit
}

object CellVisitor {
  def apply[T, R](implicit ev: CellVisitor[T, R]): CellVisitor[T, R] = ev
}
