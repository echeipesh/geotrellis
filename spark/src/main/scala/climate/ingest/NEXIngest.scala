package climate.ingest

import geotrellis.spark._
import geotrellis.spark.ingest.NetCDFIngestCommand._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.s3._
import geotrellis.spark.ingest._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.utils.SparkUtils
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.slf4j.Logging

/** Ingests the chunked NEX GeoTIFF data */
object NEXIngest extends ArgMain[AccumuloIngestArgs] with Logging {
  def main(args: AccumuloIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")
    val conf = sparkContext.hadoopConfiguration
    conf.set("io.map.index.interval", "1")

    //sparkContext.setCheckpointDir("/Users/rob/proj/climate/")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val layoutScheme = ZoomedLayoutScheme()

    val save = { (rdd: RasterRDD[SpaceTimeKey], level: LayoutLevel) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, args.clobber)
    }

    // Get source tiles
    val inPath = args.inPath
    val updatedConf =
      sparkContext.hadoopConfiguration.withInputDirectory(inPath)
    val source = 
      sparkContext.newAPIHadoopRDD(
        updatedConf,
        classOf[TemporalGeoTiffS3InputFormat],
        classOf[SpaceTimeInputKey],
        classOf[Tile]
      )

    val (level, rdd) =  Ingest[SpaceTimeInputKey, SpaceTimeKey](source, args.destCrs, layoutScheme)

    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save)
    } else{
      save(rdd, level)
    }
  }
}