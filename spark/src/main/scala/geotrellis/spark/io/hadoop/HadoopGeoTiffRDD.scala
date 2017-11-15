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

package geotrellis.spark.io.hadoop

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.tags.TiffTags
import geotrellis.spark._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.RasterReader
import geotrellis.util.{LazyLogging, StreamingByteReader}
import geotrellis.vector._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI
import java.nio.ByteBuffer


/**
  * Allows for reading of whole or windowed GeoTiff as RDD[(K, V)]s through Hadoop FileSystem API.
  */
object HadoopGeoTiffRDD extends LazyLogging {
  final val GEOTIFF_TIME_TAG_DEFAULT = "TIFFTAG_DATETIME"
  final val GEOTIFF_TIME_FORMAT_DEFAULT = "yyyy:MM:dd HH:mm:ss"

  /**
    * This case class contains the various parameters one can set when reading RDDs from Hadoop using Spark.
    *
    * @param tiffExtensions Read all file with an extension contained in the given list.
    * @param crs           Override CRS of the input files. If [[None]], the reader will use the file's original CRS.
    * @param timeTag       Name of tiff tag containing the timestamp for the tile.
    * @param timeFormat    Pattern for [[java.time.format.DateTimeFormatter]] to parse timeTag.
    * @param maxTileSize   Maximum allowed size of each tiles in output RDD.
    *                      May result in a one input GeoTiff being split amongst multiple records if it exceeds this size.
    *                      If no maximum tile size is specific, then each file file is read fully.
    * @param numPartitions How many partitions Spark should create when it repartitions the data.
    * @param chunkSize     How many bytes should be read in at a time.
    */

  case class Options(
    tiffExtensions: Seq[String] = Seq(".tif", ".TIF", ".tiff", ".TIFF"),
    crs: Option[CRS] = None,
    timeTag: String = GEOTIFF_TIME_TAG_DEFAULT,
    timeFormat: String = GEOTIFF_TIME_FORMAT_DEFAULT,
    maxTileSize: Option[Int] = None,
    numPartitions: Option[Int] = None,
    partitionBytes: Option[Long] = Some(128l * 1024 * 1024),
    chunkSize: Option[Int] = None
  ) extends RasterReader.Options

  private val DefaultMaxTileSize = 256
  private val DefaultPartitionBytes = 128l * 1024 * 1024

  object Options {
    def DEFAULT = Options()
  }

  /**
    * Create Configuration for [[BinaryFileInputFormat]] based on parameters and options.
    *
    * @param path     Hdfs GeoTiff path.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  private def configuration(path: Path, options: Options)(implicit sc: SparkContext): Configuration = {
    val conf = sc.hadoopConfiguration.withInputDirectory(path, options.tiffExtensions)
    conf
  }

  /**
    * A helper function to get the maximum (linear) tile size.
    */
  private def getMaxSize(options: Options) = {
    options.maxTileSize match {
      case Some(maxTileSize) => maxTileSize
      case None => {
        val size = Options.DEFAULT.maxTileSize match {
          case Some(maxTileSize) => maxTileSize
          case None => DefaultMaxTileSize
        }
        logger.warn(s"maxTileSize was not given, defaulting to $size.")
        size
      }
    }
  }

  /**
    * Creates a RDD[(K, V)] whose K and V depends on the type of the GeoTiff that is going to be read in.
    *
    * @param  path      HDFS GeoTiff path.
    * @param  uriToKey  Function to transform input key basing on the URI information.
    * @param  options   An instance of [[Options]] that contains any user defined or default settings.
    * @param  geometry  An optional geometry to filter by.  If this is provided, it is assumed that all GeoTiffs are in the same CRS, and that this geometry is in that CRS.
    */
  def apply[I, K, V](
    path: Path,
    uriToKey: (URI, I) => K,
    options: Options,
    geometry: Option[Geometry] = None
  )(implicit sc: SparkContext, rr: RasterReader[Options, (I, V)]): RDD[(K, V)] = {

    val conf = new SerializableConfiguration(configuration(path, options))
    val pathString = path.toString // The given path as a String instead of a Path

    if (options.maxTileSize.isDefined) {
      if (options.numPartitions.isDefined) logger.warn("numPartitions option is ignored")
      val sourceGeoTiffInfo = HadoopGeoTiffInfoReader(pathString, conf, options.tiffExtensions)

      val windows: RDD[(String, Array[GridBounds])] =
        sourceGeoTiffInfo.geoTiffInfoRdd.flatMap({ uri =>
          sourceGeoTiffInfo.windowsByPartition(
            info = sourceGeoTiffInfo.getGeoTiffInfo(uri),
            maxSize = getMaxSize(options),
            partitionBytes = options.partitionBytes.getOrElse(DefaultPartitionBytes),
            geometry = geometry
          ).map { windows => (uri, windows) }
        })


      val repartition = {
        val windowCount = windows.count.toInt
        if (windowCount > windows.partitions.length) {
          logger.info(s"Repartition into ${windowCount} partitions.")
          windows.repartition(windowCount)
        }
        else windows
      }

      val result = repartition.flatMap { case (path, windows) =>
        val info = sourceGeoTiffInfo.getGeoTiffInfo(path)
        rr.readWindows(windows, info, options).map { case (k, v) =>
          uriToKey(new URI(path), k) -> v
        }
      }

      windows.unpersist()
      result
    } else {
      sc.newAPIHadoopRDD(
        conf.value,
        classOf[BytesFileInputFormat],
        classOf[Path],
        classOf[Array[Byte]]
      ).mapPartitions(
        _.map { case (p, bytes) =>
          val (k, v) = rr.readFully(ByteBuffer.wrap(bytes), options)
          uriToKey(p.toUri, k) -> v
        },
        preservesPartitioning = true
      )
    }
  }

  /**
    * Creates a RDD[(K, V)] whose K and V depends on the type of the GeoTiff that is going to be read in.
    *
    * @param path     Hdfs GeoTiff path.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def apply[K, V](path: Path, options: Options)(implicit sc: SparkContext, rr: RasterReader[Options, (K, V)]): RDD[(K, V)] =
    apply[K, K, V](path, (_: URI, key: K) => key, options)

  /**
    * Creates a RDD[(K, V)] whose K and V depends on the type of the GeoTiff that is going to be read in.
    *
    * @param pathsToDimensions  RDD keyed by GeoTiff path with (cols, rows) tuple as value.
    * @param uriToKey           A function to transform input key basing on the URI information.
    * @param options            An instance of [[Options]] that contains any user defined or default settings.
    */
  def apply[I, K, V](
    pathsToDimensions: RDD[(Path, (Int, Int))],
    uriToKey: (URI, I) => K,
    options: Options
  )(implicit rr: RasterReader[Options, (I, V)]): RDD[(K, V)] = {

    val conf = new SerializableConfiguration(pathsToDimensions.sparkContext.hadoopConfiguration)


    val windows =
      pathsToDimensions
        .flatMap { case (path, _) =>
          val infoReader = HadoopGeoTiffInfoReader(path.toString, conf, options.tiffExtensions)
          infoReader.windowsByPartition(
            info = infoReader.getGeoTiffInfo(path.toString),
            maxSize = getMaxSize(options),
            partitionBytes = options.partitionBytes.getOrElse(DefaultPartitionBytes),
            geometry = None
          ).map { windows => (path, windows) }
        }

    windows.persist()

    val repartition = {
      val windowCount = windows.count.toInt
      if (windowCount > windows.partitions.length) {
        logger.info(s"Repartition into ${windowCount} partitions.")
        windows.repartition(windowCount)
      }
      else windows
    }


    val result = repartition.flatMap { case (path, windows) =>
      val infoReader = HadoopGeoTiffInfoReader(path.toString, conf, options.tiffExtensions)
      val info = infoReader.getGeoTiffInfo(path.toString)

      rr.readWindows(windows, info, options).map { case (k, v) =>
        uriToKey(new URI(path.toString), k) -> v
      }
    }

    windows.unpersist()
    result
  }

  /**
    * Creates RDDs with the [(K, V)] values where V is a [[Tile]].
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def singleband[I, K](path: Path, uriToKey: (URI, I) => K, options: Options)(implicit sc: SparkContext, rr: RasterReader[Options, (I, Tile)]): RDD[(K, Tile)] =
    apply[I, K, Tile](path, uriToKey, options)

  /**
    * Creates RDDs with the [(K, V)] values where V is a [[Tile]].
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def singleband[K](path: Path, options: Options)(implicit sc: SparkContext, rr: RasterReader[Options, (K, Tile)]): RDD[(K, Tile)] =
    apply[K, Tile](path, options)

  /**
    * Creates RDDs with the [(K, V)] values where V is a [[MultibandTile]].
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */

  def multiband[I, K](path: Path, uriToKey: (URI, I) => K, options: Options)(implicit sc: SparkContext, rr: RasterReader[Options, (I, MultibandTile)]): RDD[(K, MultibandTile)] =
    apply[I, K, MultibandTile](path, uriToKey, options)

  /**
    * Creates RDDs with the [(K, V)] values where V is a [[MultibandTile]].
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def multiband[K](path: Path, options: Options)(implicit sc: SparkContext, rr: RasterReader[Options, (K, MultibandTile)]): RDD[(K, MultibandTile)] =
    apply[K, MultibandTile](path,options)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    */
  def spatial(path: Path)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] =
    spatial(path, Options.DEFAULT)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def spatial(path: Path, options: Options)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] =
    singleband[ProjectedExtent](path, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def spatial(path: Path, uriToKey: (URI, ProjectedExtent) => ProjectedExtent, options: Options)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] =
    singleband[ProjectedExtent, ProjectedExtent](path, uriToKey, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    */
  def spatialMultiband(path: Path)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] =
    spatialMultiband(path, Options.DEFAULT)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def spatialMultiband(path: Path, options: Options)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] =
    multiband[ProjectedExtent](path, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[ProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def spatialMultiband(path: Path, uriToKey: (URI, ProjectedExtent) => ProjectedExtent, options: Options)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] =
    multiband[ProjectedExtent, ProjectedExtent](path, uriToKey, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    */
  def temporal(path: Path)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, Tile)] =
    temporal(path, Options.DEFAULT)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def temporal(path: Path, options: Options)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, Tile)] =
    singleband[TemporalProjectedExtent](path, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[Tile]], respectively.
    * It assumes that the provided files are [[SinglebandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def temporal(path: Path, uriToKey: (URI, TemporalProjectedExtent) => TemporalProjectedExtent, options: Options)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, Tile)] =
    singleband[TemporalProjectedExtent, TemporalProjectedExtent](path, uriToKey, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    */
  def temporalMultiband(path: Path)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, MultibandTile)] =
    temporalMultiband(path, Options.DEFAULT)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def temporalMultiband(path: Path, options: Options)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, MultibandTile)] =
    multiband[TemporalProjectedExtent](path, options)

  /**
    * Creates RDDs with the [(K, V)] values being [[TemporalProjectedExtent]] and [[MultibandTile]], respectively.
    * It assumes that the provided files are [[MultibandGeoTiff]]s.
    *
    * @param path     Hadoop path to recursively search for GeoTiffs.
    * @param uriToKey function to transform input key basing on the URI information.
    * @param options  An instance of [[Options]] that contains any user defined or default settings.
    */
  def temporalMultiband(path: Path, uriToKey: (URI, TemporalProjectedExtent) => TemporalProjectedExtent, options: Options)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, MultibandTile)] =
    multiband[TemporalProjectedExtent, TemporalProjectedExtent](path, uriToKey, options)
}
