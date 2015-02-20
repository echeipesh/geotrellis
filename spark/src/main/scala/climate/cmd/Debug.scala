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
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.reproject._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.vector.json._
import geotrellis.spark.op.zonal.summary._
import geotrellis.raster.op.zonal.summary._
import geotrellis.spark.op.stats._
import com.github.nscala_time.time.Imports._
import geotrellis.raster.op.local


object Debug extends ArgMain[BenchmarkArgs] with Logging {
  import Extents._
  
  def zonalSummary(rdd: RasterRDD[SpaceTimeKey], polygon: Polygon) = {
    rdd
      .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
      .averageByKey
      .zonalSummaryByKey(polygon, Double.MinValue, MaxDouble, stk => stk.temporalComponent.time)
      .collect
      .sortBy(_._1)
  }

  def main(args: BenchmarkArgs): Unit = {
    implicit val sparkContext = SparkUtils.createSparkContext("Benchmark")

    val layers = args.getLayers

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    println("------ Single Model Benchmark ------")
    for { 
      (name, polygon) <- extents if name == "Rockies"      
      count <- 1 to 2
    } {
      Timer.timedTask(s"TOTAL Two-State Single: $name"){  
        
        val (lmd, params) = catalog.metaDataCatalog.load(layers.head)
        val md = lmd.rasterMetaData  
        val bounds = md.mapTransform(polygon.envelope)
        
        val rdd1 = catalog.load[SpaceTimeKey](layers.head, FilterSet(SpaceFilter[SpaceTimeKey](bounds))).cache
      
        Timer.timedTask(s"- Load Tiles"){
          rdd1.foreachPartition( _ => {})
        }

        Timer.timedTask(s"- Zonal Summary Calclutation") {
          zonalSummary(rdd1, polygon)      
        }     
        rdd1.unpersist()
      }

      Timer.timedTask(s"TOTAL One-State Single: $name"){  
        
        val (lmd, params) = catalog.metaDataCatalog.load(layers.head)
        val md = lmd.rasterMetaData  
        val bounds = md.mapTransform(polygon.envelope)
        
        val rdd1 = catalog.load[SpaceTimeKey](layers.head, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))
      
        zonalSummary(rdd1, polygon)      
      } 
    }
  }
}
