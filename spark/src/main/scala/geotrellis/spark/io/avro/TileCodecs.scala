package geotrellis.spark.io.avro

import geotrellis.raster._
import org.apache.avro.generic._
import org.apache.avro.SchemaBuilder
import scala.collection.JavaConverters._
import java.nio.ByteBuffer

object TileCodecs {
  implicit object ShortArrayTileCodec extends AvroRecordCodec[ShortArrayTile] {
    lazy val schema = SchemaBuilder
      .record("ShortArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().intType().noDefault()
      .endRecord()

    def encode(tile: ShortArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      // _* expansion is important, otherwise we get List[Array[Short]] instead of List[Short]
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells")
        .asInstanceOf[java.util.Collection[Int]]
        .asScala // notice that Avro does not have native support for Short primitive
        .map(_.toShort)
        .toArray
      new ShortArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit object IntArrayTileCodec extends AvroRecordCodec[IntArrayTile] {
    lazy val schema = SchemaBuilder
      .record("IntArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().intType().noDefault()
      .endRecord()

    def encode(tile: IntArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Int]].asScala.toArray[Int]
      new IntArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit object FloatArrayTileCodec extends AvroRecordCodec[FloatArrayTile] {
    lazy val schema = SchemaBuilder
      .record("FloatArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().floatType().noDefault()
      .endRecord()

    def encode(tile: FloatArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Float]].asScala.toArray[Float]
      new FloatArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit object DoubleArrayTileCodec extends AvroRecordCodec[DoubleArrayTile] {
    lazy val schema = SchemaBuilder
      .record("DoubleArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().doubleType().noDefault()
      .endRecord()

    def encode(tile: DoubleArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Double]].asScala.toArray[Double]
      new DoubleArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit object ByteArrayTileCodec extends AvroRecordCodec[ByteArrayTile] {
    lazy val schema = SchemaBuilder
      .record("ByteArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().bytesType().noDefault()
      .endRecord()

    def encode(tile: ByteArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", ByteBuffer.wrap(tile.array))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[ByteBuffer].array()
      new ByteArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }


}
