package geotrellis.spark.io.s3

import java.io.InputStream
import java.nio.ByteBuffer
import geotrellis.raster.Tile
import com.github.nscala_time.time.Imports._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.ingest._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.vector.Extent
import geotrellis.proj4._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import scala.io.Source
import scala.collection.mutable.Buffer

case class SpaceTimeInputKey(extent: Extent, crs: CRS, time: DateTime)

object SpaceTimeInputKey {
  implicit def ingestKey = new KeyComponent[SpaceTimeInputKey, ProjectedExtent] {
    def lens = createLens(
      key => ProjectedExtent(key.extent, key.crs),
      pe => key => SpaceTimeInputKey(pe.extent, pe.crs, key.time)
    )
  }

  implicit def tiler: Tiler[SpaceTimeInputKey, SpaceTimeKey] = {
    val getExtent = (inKey: SpaceTimeInputKey) => inKey.extent
    val createKey = (inKey: SpaceTimeInputKey, spatialComponent: SpatialKey) =>
      SpaceTimeKey(spatialComponent, inKey.time)

    Tiler(getExtent, createKey)
  }
}

class TemporalGeoTiffS3InputFormat extends S3InputFormat[SpaceTimeInputKey,Tile] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = 
    new S3RecordReader[SpaceTimeInputKey,Tile] {
      def read(in: InputStream) = {        
        val geoTiff = GeoTiffReader.read(in)
        val meta = geoTiff.metaData
        val isoString = geoTiff.tags("ISO_TIME")
        val dateTime = DateTime.parse(isoString)

        //WARNING: Assuming this is a single band GeoTiff
        val GeoTiffBand(tile, extent, crs, _) = geoTiff.bands.head
        (SpaceTimeInputKey(extent, crs, dateTime), tile)        
      }
    }     
}