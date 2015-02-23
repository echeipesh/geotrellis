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
import geotrellis.raster._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._
import geotrellis.spark.op.local._

class BenchmarkArgs extends AccumuloArgs {
  /** Comma seprated list of layerId:Zoom */
  @Required var layers: String = _

  def getLayers: Array[LayerId] = 
    layers
      .split(",")
      .map{ str => 
        val Array(name, zoom) = str.split(":")
        LayerId(name, zoom.toInt)
      }
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
          [-75.2947998046875,39.863371338285305]]]}}""".parseJson.convertTo[Polygon].reproject(LatLng, WebMercator),
    "eastKansas" ->
      """{"type":"Feature","properties":{"name":9},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-98.26171875,37.055177106660814],
            [-98.26171875,39.9434364619742],
            [-94.6142578125,39.9434364619742],
            [-94.6142578125,37.055177106660814],
            [-98.26171875,37.055177106660814]]]}}""".parseJson.convertTo[Polygon].reproject(LatLng, WebMercator),
    "Rockies" -> 
      """{"type":"Feature","properties":{"name":3},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-120.23437499999999,32.69746078939034],
            [-120.23437499999999,48.19643332981063],
            [-107.9296875,48.19643332981063],
            [-107.9296875,32.69746078939034],
            [-120.23437499999999,32.69746078939034]]]}}""".parseJson.convertTo[Polygon].reproject(LatLng, WebMercator),
    "USA" -> 
      """{"type":"Feature","properties":{"name":3},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-124.9132294655,25.6804735519],
            [-124.9132294655,49.2204934537],
            [-66.6759185791,49.2204934537],
            [-66.6759185791,25.6804735519],
            [-124.9132294655,25.6804735519]]]}}""".parseJson.convertTo[Polygon].reproject(LatLng, WebMercator)
  )
}

object Benchmark extends ArgMain[BenchmarkArgs] with Logging {
  import Extents._
  
  implicit val sparkContext = SparkUtils.createSparkContext("Benchmark")

  def getRdd(catalog: AccumuloCatalog, id: LayerId, polygon: Polygon, name: String): RasterRDD[SpaceTimeKey] = {
    val (lmd, params) = catalog.metaDataCatalog.load(id)
    val md = lmd.rasterMetaData  
    val bounds = md.mapTransform(polygon.envelope)        
    val rdd = catalog.load[SpaceTimeKey](id, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))
    rdd.setName(name)
    rdd
  }

  def zonalSummary(rdd: RasterRDD[SpaceTimeKey], polygon: Polygon) = {
    rdd
      .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
      .averageByKey
      .zonalSummaryByKey(polygon, Double.MinValue, MaxDouble, stk => stk.temporalComponent.time)
      .collect
      .sortBy(_._1)
  }

  def annualAverage(rdd: RasterRDD[SpaceTimeKey]): Seq[(org.joda.time.DateTime.Property, Double)] = {
    rdd
      .map{ case (key, tile) =>         
        var total: Double = 0
        var count = 0L
        tile.foreachDouble{ d => 
          if(isNoData(d)) {
            total += d
            count += 1
          }
        }
        val year = key.temporalComponent.time.year
        year -> (total, count)
      }
      .reduceByKey{ (tup1, tup2) => 
        (tup1._1 + tup2._1) -> (tup1._2 + tup2._2)
      }
      .collect
      .map{ case (year, (sum, count)) => 
        year -> sum/count 
      }
  }

  def stats(rdd: RasterRDD[SpaceTimeKey]): String = {
    val crdd = rdd.cache()
    val tiles = rdd.count
    val cells = tiles * rdd.metaData.tileLayout.tileSize
    crdd.unpersist()
    s"tiles=$tiles, cells=$cells"
  }


  def main(args: BenchmarkArgs): Unit = {
    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    val layers = args.getLayers

    for { 
      (name, polygon) <- extents
      count <- 1 to (if (name == "philadelphia") 4 else 1)
    } {
      val rdd = getRdd(catalog, layers.head, polygon, name)
    
      Timer.timedTask(s"Single: $name - Load and Cache Tiles", s => logger.info(s)){        
        logger.info(s"Stats: $name = (${stats(crdd)})")        
      }

      Timer.timedTask(s"Single: $name - zonalSummary", s => logger.info(s)) {
        zonalSummary(rdd, polygon)      
      }
    }

    for { 
      (name, polygon) <- extents
    } {    
      val rdds = layers.map { layer => getRdd(catalog, layer, polygon, name)}

      Timer.timedTask(s"Multi-Model $name .averageByKey for: ${layers.toList}", s => logger.info(s)) {
        new RasterRDD[SpaceTimeKey](rdds.reduce(_ union _), rdds.head.metaData)
          .averageByKey
          .foreachPartition(_ => {})
      }

      Timer.timedTask(s"Multi-Model $name .combineTiles(local.Mean.apply) for: ${layers.toList}", s => logger.info(s)) {
        new RasterRDD[SpaceTimeKey](rdds.reduce(_ union _), rdds.head.metaData)
          rdds.head.combineTiles(rdds.tail)(local.Mean.apply)
          .foreachPartition(_ => {})
      }

      Timer.timedTask(s"Multi-Model $name annual average difference by year", s => logger.info(s)) {
        val diff:RasterRDD[SpaceTimeKey] = rdds(1) localSubtract rdds(0)
        val aadiff = annualAverage(diff)
        logger.info(s"Annual average difference: $aadiff")
      }
    }
  }
}
