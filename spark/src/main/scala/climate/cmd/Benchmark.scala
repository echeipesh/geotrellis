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
import geotrellis.vector._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.vector.json._
import geotrellis.spark.op.zonal.summary._
import geotrellis.raster.op.zonal.summary._
import geotrellis.spark.op.stats._
import com.github.nscala_time.time.Imports._

class BenchmarkArgs extends AccumuloArgs {
}

object Extents extends GeoJsonSupport {
  import spray.json._  
  val extents = Map[String, Polygon](
    "philadelphia" -> 
      """{"type":"Feature","properties":{"name":6},"geometry":{
        "type":"Polygon",
        "coordinates":[[
          [-75.2947998046875,39.863371338285305],
          [-75.2947998046875,40.04023218690448],
          [-74.9432373046875,40.04023218690448],
          [-74.9432373046875,39.863371338285305],
          [-75.2947998046875,39.863371338285305]]]}}""".parseJson.convertTo[Polygon],
    "eastKansas" ->
      """{"type":"Feature","properties":{"name":9},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-98.26171875,37.055177106660814],
            [-98.26171875,39.9434364619742],
            [-94.6142578125,39.9434364619742],
            [-94.6142578125,37.055177106660814],
            [-98.26171875,37.055177106660814]]]}}""".parseJson.convertTo[Polygon],
    "Rockies" -> 
      """{"type":"Feature","properties":{"name":3},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-120.23437499999999,32.69746078939034],
            [-120.23437499999999,48.19643332981063],
            [-107.9296875,48.19643332981063],
            [-107.9296875,32.69746078939034],
            [-120.23437499999999,32.69746078939034]]]}}""".parseJson.convertTo[Polygon]
  )
}

object Benchmark extends ArgMain[BenchmarkArgs] with Logging {
  import Extents._
  val zoom = 8
  val layer1 = LayerId("pr-rcp26-ccsm4", zoom)
  val layer2 = LayerId("pr-rcp45-ccsm4", zoom)
  
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

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    println("------ Single Model Benchmark ------")
    for ( (name, polygon) <- extents) {
      val (lmd, params) = catalog.metaDataCatalog.load(layer1)
      val md = lmd.rasterMetaData  
      val bounds = md.mapTransform(polygon.envelope)
      val rdd1 = catalog.load[SpaceTimeKey](layer1, FilterSet(SpaceFilter[SpaceTimeKey](bounds))).cache
    
      Timer.timedTask(s"Load $name"){
        rdd1.count
      }

      Timer.timedTask(s"Zonal Summary $name") {
        zonalSummary(rdd1, polygon)      
      }
    }

    println("------ Multi-Model Benchmark ------")
    for ( (name, polygon) <- extents) {
    
      val rdd1 = {
        val (lmd, params) = catalog.metaDataCatalog.load(layer1)
        val md = lmd.rasterMetaData  
        val bounds = md.mapTransform(polygon.envelope)
        catalog.load[SpaceTimeKey](layer1, FilterSet(SpaceFilter[SpaceTimeKey](bounds))).cache
      }
      val rdd2 = {
        val (lmd, params) = catalog.metaDataCatalog.load(layer2)
        val md = lmd.rasterMetaData  
        val bounds = md.mapTransform(polygon.envelope)
        catalog.load[SpaceTimeKey](layer2, FilterSet(SpaceFilter[SpaceTimeKey](bounds))).cache
      }

    
      Timer.timedTask(s"Load rdd1 $name"){
        rdd1.count
      }
      Timer.timedTask(s"Load rdd2 $name"){
        rdd1.count
      }


      Timer.timedTask(s"Multi-Model Average for $name") {
        new RasterRDD[SpaceTimeKey](rdd1.union(rdd2), rdd1.metaData)
          .averageByKey
          .foreachPartition(_ => {})
      }
    }
  }
}
