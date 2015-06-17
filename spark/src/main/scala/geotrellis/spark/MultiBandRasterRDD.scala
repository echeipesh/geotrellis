/*
 * Copyright (c) 2015 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark

import geotrellis.raster._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag

class MultiBandRasterRDD[K: ClassTag](tileRdd: RDD[(K, MultiBandTile)], val metaData: RasterMetaData) extends GeoRDD[K, MultiBandTile](tileRdd) {
  type Self = MultiBandRasterRDD[K]
  
  def wrap(f: => RDD[(K, MultiBandTile)]): MultiBandRasterRDD[K] =
    new MultiBandRasterRDD[K](f, metaData)

  def convert(cellType: CellType): Self =
    mapTiles(_.convert(cellType))  
}