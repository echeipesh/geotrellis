package climate.cmd

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required
import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.ingest.IngestArgs
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.op.stats._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector.Extent
import org.apache.accumulo.core.client.security.tokens.PasswordToken

import org.apache.spark._

class CalculateArgs extends AccumuloArgs {
  @Required var inputLayer: String = _
  @Required var outputLayer: String = _
}

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object Calculate extends ArgMain[CalculateArgs] with Logging {
  def main(args: CalculateArgs): Unit = {
    implicit val sparkContext = SparkUtils.createSparkContext("Calculate")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    val rdd = catalog.load[SpaceTimeKey](LayerId(args.inputLayer, 2)).get
    
    // val ret = rdd
    //   .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
    //   .averageByKey
    // catalog.save[SpaceTimeKey](LayerId(args.outputLayer,2), "results", ret, true).get

    import geotrellis.spark.op.zonal.summary._
    import geotrellis.raster.op.zonal.summary._
    val polygon = Extent(-13193016.062816, 3088377.007329,
                          -8453323.832114, 5722687.564266)
    
    val min = rdd.zonalSummaryByKey(polygon, Int.MinValue, Min, stk => stk.temporalComponent.time)
    val max = rdd.zonalSummaryByKey(polygon, Int.MinValue, Max, stk => stk.temporalComponent.time)
    val mean = rdd.zonalSummaryByKey(polygon, MeanResult(0.0, 0L), Mean, (a: MeanResult, b: MeanResult) => a + b, stk => stk.temporalComponent.time)
    min zip max zip mean foreach (println)
  }
}
