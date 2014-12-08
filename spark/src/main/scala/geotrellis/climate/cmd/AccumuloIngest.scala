package climate.cmd

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.ingest.{Ingest, Pyramid, AccumuloIngestArgs}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.utils.SparkUtils

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark._
import com.quantifind.sumac.ArgMain

object AccumuloIngestCommand extends ArgMain[AccumuloIngestArgs] with Logging {
  def main(args: AccumuloIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val source = sparkContext.netCdfRDD(args.inPath).repartition(args.partitions)

    val layoutScheme = ZoomedLayoutScheme(256)
    val (level, rdd) =  Ingest[NetCdfBand, SpaceTimeKey](source, args.destCrs, layoutScheme)

    val save = { (rdd: RasterRDD[SpaceTimeKey], level: LayoutLevel) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, args.clobber)
    }

    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save).get // expose exceptions
    } else{
      save(rdd, level).get
    }
  }
}

~/spark-1.1.1-bin-cdh4/bin/spark-submit \
--class climate.cmd.AccumuloIngestCommand \
--conf spark.mesos.coarse=true \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--crs EPSG:3857 --instance gis --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal. \
--input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/tas/miroc5/rcp45/BCSD_0.5deg_tas_Amon_miroc5_rcp45_r1i1p1_200601-210012.nc \
--layerName tas-miroc5-rcp45 \
--table temp