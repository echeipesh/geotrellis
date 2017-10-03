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

package geotrellis.spark.io.file

import geotrellis.spark.io.avro.{AvroRecordCodec, AvroEncoder}
import geotrellis.spark.io.avro.codecs.KeyValueRecordCodec
import geotrellis.spark.util.KryoWrapper
import geotrellis.util.Filesystem

import org.apache.spark.rdd.RDD
import org.apache.avro.Schema

import scala.reflect.ClassTag

import java.io.File


object FileRDDWriter {
  private[file] def update[K: AvroRecordCodec: ClassTag, V: AvroRecordCodec: ClassTag](
    rdd: RDD[(K, V)],
    rootPath: String,
    keyPath: K => String,
    writerSchema: Option[Schema],
    mergeFunc: Option[(V,V) => V]
  ): Unit = {
    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema

    val pathsToTiles: RDD[(String, Iterable[(K, V)])] =
      // Call groupBy with numPartitions; if called without that argument or a partitioner,
      // groupBy will reuse the partitioner on the parent RDD if it is set, which could be typed
      // on a key type that may no longer by valid for the key type of the resulting RDD.
      rdd.groupBy({ row: (K, V) => keyPath(row._1) }, numPartitions = rdd.partitions.length)

    Filesystem.ensureDirectory(rootPath)
    val _recordCodec = KeyValueRecordCodec[K, V]
    val kwWriterSchema = KryoWrapper(writerSchema)

    pathsToTiles.foreach { case (path, rows) =>
      val rows1: Vector[(K,V)] = rows.toVector
      val rows2: Vector[(K,V)] =
        if (mergeFunc.nonEmpty && Filesystem.exists(path)) {
          val inBytes = Filesystem.slurp(path)
          AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), inBytes)(_recordCodec)
        }
        else Vector.empty
      val outRows: Vector[(K, V)] = mergeFunc match {
        case Some(fn) =>
          (rows2 ++ rows1)
            .groupBy({ case (k,v) => k })
            .map({ case (k, kvs) =>
              val vs = kvs.map({ case (k,v) => v }).toSeq
              val v: V = vs.tail.foldLeft(vs.head)(fn)
              (k, v) })
            .toVector
        case None => rows1
      }
      val outBytes: Array[Byte] = AvroEncoder.toBinary(outRows)(codec)
      Filesystem.writeBytes(path, outBytes)
    }
  }

  def write[K: AvroRecordCodec: ClassTag, V: AvroRecordCodec: ClassTag](
    rdd: RDD[(K, V)],
    rootPath: String,
    keyPath: K => String
  ): Unit = update(rdd, rootPath, keyPath, None, None)
}
