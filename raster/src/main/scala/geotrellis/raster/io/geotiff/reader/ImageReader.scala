/*
 * Copyright (c) 2014 Azavea.
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

package geotrellis.raster.io.geotiff.reader

import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster.io.geotiff.reader.CompressionType._
import geotrellis.raster.io.geotiff.reader.decompression._

import monocle.syntax._

import java.nio.{ByteBuffer, ByteOrder}

import spire.syntax.cfor._

object ImageReader {

  def read(byteBuffer: ByteBuffer, directory: ImageDirectory): ImageDirectory = {

    // TODO: NEEDS HEAVY OPTIMIZATION
    def readSections(offsets: Array[Int], byteCounts: Array[Int]): Array[Array[Byte]] = {
      val oldOffset = byteBuffer.position

      val result = Array.ofDim[Array[Byte]](offsets.size)

      cfor(0)(_ < offsets.size, _ + 1) { i =>
        byteBuffer.position(offsets(i))
        result(i) = byteBuffer.getSignedByteArray(byteCounts(i))
      }

      byteBuffer.position(oldOffset)

      result
    }

    def readMatrix(directory: ImageDirectory): Array[Array[Byte]] =
      if (directory.hasStripStorage) readStrips(directory)
      else readTiles(directory)

    def readStrips(directory: ImageDirectory): Array[Array[Byte]] = {
      val stripOffsets = (directory &|->
        ImageDirectory._basicTags ^|->
        BasicTags._stripOffsets get)

      val stripByteCounts = (directory &|->
        ImageDirectory._basicTags ^|->
        BasicTags._stripByteCounts get)

      readSections(stripOffsets.get, stripByteCounts.get)
    }

    def readTiles(directory: ImageDirectory) = {
      val tileOffsets = (directory &|->
        ImageDirectory._tileTags ^|->
        TileTags._tileOffsets get)

      val tileByteCounts = (directory &|->
        ImageDirectory._tileTags ^|->
        TileTags._tileByteCounts get)

      readSections(tileOffsets.get, tileByteCounts.get)
    }

    val matrix = readMatrix(directory)


    val uncompressedImage: Array[Array[Byte]] =
      directory.compression match {
        case Uncompressed => matrix
        case HuffmanCoded => matrix.uncompressHuffman(directory)
        case GroupThreeCoded => matrix.uncompressGroupThree(directory)
        case GroupFourCoded => matrix.uncompressGroupFour(directory)
        case LZWCoded => matrix.uncompressLZW(directory)
        case JpegCoded => matrix.uncompressJpeg(directory)
        case ZLibCoded | PkZipCoded => matrix.uncompressZLib(directory)
        case PackBitsCoded => matrix.uncompressPackBits(directory)
        case JpegOldCoded =>
          val msg = "old jpeg (compression = 6) is deprecated."
          throw new MalformedGeoTiffException(msg)
        case compression =>
          val msg = s"compression type $compression is not supported by this reader."
          throw new GeoTiffReaderLimitationException(msg)
      }


    val imageBytes = ImageConverter(directory,
      byteBuffer.order == ByteOrder.BIG_ENDIAN).convert(uncompressedImage)

    (directory &|-> ImageDirectory._imageBytes set(imageBytes))
  }


}
