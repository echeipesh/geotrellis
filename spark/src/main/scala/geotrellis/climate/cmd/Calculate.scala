package climate.cmd

import climate.op.PredicateCount
import com.quantifind.sumac.ArgMain
import geotrellis.raster._

import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.utils.SparkUtils
import org.apache.accumulo.core.client.security.tokens.PasswordToken

import org.apache.hadoop.fs.Path
import org.apache.spark._
import climate.utils.Utils

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object Calculate extends ArgMain[AccumuloArgs] with Logging {
  def main(args: AccumuloArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    val rdd = catalog.load[SpaceTimeKey](LayerId("pr-rcp45", 2)).get
    
    val predicate = { pr: Double => if (isNoData(pr)) Double.NaN else if (pr < 1.0) 1 else 0 }
    val groupBy =  { key: SpaceTimeKey => key.updateTemporalComponent(key.temporalKey.time.withDayOfMonth(1).withMonthOfYear(1).withHourOfDay(0))}
    val ret = PredicateCount(TypeByte, predicate, groupBy)(rdd);
    catalog.save[SpaceTimeKey](LayerId("pr-very-dry",2), "results", ret, true).get
  }
}
