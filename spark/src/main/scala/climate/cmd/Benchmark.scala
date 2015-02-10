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

class BenchmarkArgs extends AccumuloArgs {
}

object Extents extends GeoJsonSupport {
  import spray.json._  
  
  val philadelphia = 
    """{"type":"Feature","properties":{"name":6},"geometry":{
      "type":"Polygon",
      "coordinates":[[
        [-75.2947998046875,39.863371338285305],
        [-75.2947998046875,40.04023218690448],
        [-74.9432373046875,40.04023218690448],
        [-74.9432373046875,39.863371338285305],
        [-75.2947998046875,39.863371338285305]]]}}""".parseJson.convertTo[Polygon]
  
  val eastKansas = 
  """{"type":"Feature","properties":{"name":9},"geometry":{
      "type":"Polygon",
      "coordinates":[[
        [-98.26171875,37.055177106660814],
        [-98.26171875,39.9434364619742],
        [-94.6142578125,39.9434364619742],
        [-94.6142578125,37.055177106660814],
        [-98.26171875,37.055177106660814]]]}}""".parseJson.convertTo[Polygon]

}

object Benchmark extends ArgMain[BenchmarkArgs] with Logging {
  import Extents._
  val zoom = 8
  val layer1 = LayerId("pr-rcp26-ccsm4", zoom)
  val layer2 = LayerId("pr-rcp45-ccsm4", zoom)
  

  def main(args: BenchmarkArgs): Unit = {
    implicit val sparkContext = SparkUtils.createSparkContext("Benchmark")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog


    val (lmd, params) = catalog.metaDataCatalog.load(layer1)
    val md = lmd.rasterMetaData  
    val polygon = philadelphia
    val bounds = md.mapTransform(polygon.envelope)
    val tiles = catalog.load[SpaceTimeKey](layer1, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))    
    tiles
      .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
      .averageByKey
      
  }
}
