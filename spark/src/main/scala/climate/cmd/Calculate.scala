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
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.index.zcurve._

class CalculateArgs extends AccumuloArgs {
  @Required var inputLayer: String = _
  @Required var outputLayer: String = _
}

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object Calculate extends ArgMain[BenchmarkArgs] with Logging {
  def main(args: BenchmarkArgs): Unit = {
    implicit val sparkContext = SparkUtils.createSparkContext("Calculate")
    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog
    val layer = args.getLayers.head
    println(s"Working with: $layer")
    val rdd: RasterRDD[SpaceTimeKey] = Benchmark.getRdd(catalog, layer, Extents.extents("USA"), "USA")
    
    println(s"Record Count: ${rdd.count}")
    val things = rdd.mapPartitionsWithIndex( (idx, iter) => {
      var min: Long = Long.MaxValue
      var max: Long = Long.MinValue
      
      iter.foreach{ case (key, tile) =>
        val z = Z3(key.spatialKey.col, key.spatialKey.row, key.temporalKey.time.getYear).z
        if (z > max) max = z
        if (z < min) min = z
      }
      Iterator(idx -> (min, max))
    }, true).collect
    println("Partition min/max:")
    things.foreach{println}

  }
}
