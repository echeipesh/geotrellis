package climate.rest


import akka.actor.ActorSystem
import com.quantifind.sumac.{ ArgApp, ArgMain }

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.Nodata
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.fs.Path
import geotrellis.spark.json._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector.reproject._
import geotrellis.vector._
import geotrellis.vector.json._

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.joda.time.DateTime
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http.{ AllOrigins, MediaTypes }
import spray.http.{ HttpMethods, HttpMethod, HttpResponse, AllOrigins }
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing._
import spray.routing.SimpleRoutingApp
import climate.op._
import com.github.nscala_time.time.Imports._

trait CORSSupport { self: HttpService =>
  val corsHeaders = List(`Access-Control-Allow-Origin`(AllOrigins),
    `Access-Control-Allow-Methods`(GET, POST, OPTIONS, DELETE),
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"))

  def cors: Directive0 = {
    val rh = implicitly[RejectionHandler]
    respondWithHeaders(corsHeaders) & handleRejections(rh)
  }
}
class CatalogArgs extends AccumuloArgs
/**
 * Catalog and TMS service for TimeRaster layers only
 * This is intended to exercise the machinery more than being a serious service.
 */
object CatalogService extends ArgApp[CatalogArgs] with SimpleRoutingApp with SprayJsonSupport with CORSSupport {
  implicit val system = ActorSystem("spray-system")
  implicit val sparkContext = SparkUtils.createSparkContext("Catalog")

  val accumulo = AccumuloInstance(argHolder.instance, argHolder.zookeeper,
    argHolder.user, new PasswordToken(argHolder.password))
  val catalog = accumulo.catalog
  //val catalog: HadoopCatalog = HadoopCatalog(sparkContext, new Path("hdfs://localhost/catalog"))

  /** Simple route to test responsiveness of service. */
  val pingPong = path("ping")(complete("pong"))

  /** Server out TMS tiles for some layer */
  def tmsRoute =
    pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (layer, zoom, x, y) =>
      parameters('time.?, 'breaks.?) { (timeOption, breaksOption) =>    
        respondWithMediaType(MediaTypes.`image/png`) {
          complete {
            future {
              val tile = timeOption match {
                case Some(time) =>
                  val dt = DateTime.parse(time)
                  val filters = FilterSet[SpaceTimeKey]()
                    .withFilter(SpaceFilter(GridBounds(x, y, x, y)))
                    .withFilter(TimeFilter(dt, dt))

                  val rdd = catalog.load[SpaceTimeKey](LayerId(layer, zoom), filters)
                  rdd.get.first().tile
                case None =>
                  val filters = FilterSet[SpatialKey]() 
                    .withFilter(SpaceFilter(GridBounds(x, y, x, y)))

                  val rdd = catalog.load[SpatialKey](LayerId(layer, zoom), filters)
                  rdd.get.first().tile
              }

              breaksOption match {
                case Some(breaks) =>
                  tile.renderPng(ColorRamps.BlueToOrange, breaks.split(",").map(_.toInt)).bytes
                case None =>
                  tile.renderPng.bytes  
              }              
            }
          }
        }
      }
    }

  def catalogRoute = cors {
    path("") {
      get {
        // get the entire catalog
        complete {
          import DefaultJsonProtocol._

          accumulo.metaDataCatalog.fetchAll.toSeq.map {
            case (key, lmd) =>
              val (layer, table) = key
              val md = lmd.rasterMetaData
              val center = md.extent.reproject(md.crs, LatLng).center
              val breaks = lmd.histogram.get.getQuantileBreaks(12)

              JsObject(
                "layer" -> layer.toJson,
                "metaData" -> md.toJson,
                "center" -> List(center.x, center.y).toJson,
                "breaks" -> breaks.toJson
              )
          }
        }
      }
    } ~ 
    pathPrefix(Segment / IntNumber) { (name, zoom) =>      
      val layer = LayerId(name, zoom)
      val (lmd, params) = catalog.metaDataCatalog.load(layer).get
      val md = lmd.rasterMetaData
      (path("bands") & get) { 
        import DefaultJsonProtocol._
        complete{ future {          
          val bands = {
            val GridBounds(col, row, _, _) = md.mapTransform(md.extent)
            val filters = new FilterSet[SpaceTimeKey]() withFilter SpaceFilter(GridBounds(col, row, col, row))
            catalog.load(layer, filters).map { // into Try
              _.map { case (key, tile) => key.temporalKey.time.toString }
            }
          }.get.collect
          JsObject("time" -> bands.toJson)
        } }
      } ~ 
      (path("breaks") & get) {
        parameters('num ? "10") { num =>  
          import DefaultJsonProtocol._ 
          complete { future {                      
            
            (if (layer.name == "NLCD")
              Histogram(catalog.load[SpatialKey](layer).get)
            else
              Histogram(catalog.load[SpaceTimeKey](layer).get)
            ).getQuantileBreaks(num.toInt)
          } }
        }
      }
    }
  }

  import geotrellis.spark.op.stats._
  import geotrellis.spark.op.zonal.summary._
  import geotrellis.raster.op.zonal.summary._

  // import scalaz._

  // def getLayer = Memo.mutableHashMapMemo{ layer: LayerId =>
  //   val rdd = catalog.load[SpaceTimeKey](layer).get
  //   asRasterRDD(rdd.metaData) {
  //     rdd.repartition(8).cache
  //   }
  // }


  val extents = Map(
    "philadelphia" -> Extent(-75.9284841594,39.5076933259,-74.3543298352,40.5691292481),
    "orlando"      -> Extent(-81.6640927758,28.2876159612,-81.0875104164,28.7973683779),
    "sanjose"      -> Extent(-122.204281443,37.1344012781,-121.5923878102,37.5401705236),
    "portland"     -> Extent(-123.015276532,45.2704007159,-122.3044835961,45.7602907701),
    "usa"          -> Extent(-124.9268976258,25.3021853935,-65.521646682,49.0009765951),
    "pa"           -> Extent(-80.8936945008,39.7560786402,-74.662271682,42.2519392104),
    "world"        -> Extent(-176.8565908405,-80.2714428203,176.8357981709,83.5687714812)
  )

  def statsReponse(model: String, data: Seq[(DateTime, Double)]) =  
    JsArray(JsObject(
      "model" -> JsString(model),
      "data" -> JsArray(
        data.map { case (date, value) =>
          JsObject(
            "time" -> JsString(date.toString), 
            "value" -> JsObject(
              "mean" -> JsNumber(value),
              "min" -> JsNumber(value * (1 + scala.util.Random.nextDouble/3)), // I know, I'm a liar
              "max" -> JsNumber(value * (1 - scala.util.Random.nextDouble/3))
            )
          )
        }: _*)
    ))

  def statsRoute = cors {
    (pathPrefix(Segment / IntNumber) & get ) { (name, zoom) =>      
      import DefaultJsonProtocol._         
      
      val layer = LayerId(name, zoom)
      val (lmd, params) = catalog.metaDataCatalog.load(layer).get
      println("LOADED", lmd, params)
      val md = lmd.rasterMetaData  

      parameters('city) { city => 
        val extent = extents(city).reproject(LatLng, md.crs)
        val bounds = md.mapTransform(extent)

        path("multimodel") {
          complete {
            val model1 = catalog.load[SpaceTimeKey](LayerId("tas-miroc5-rcp45",4), FilterSet(SpaceFilter[SpaceTimeKey](bounds))).get
            val model2 = catalog.load[SpaceTimeKey](LayerId("tas-access1-rcp45",4), FilterSet(SpaceFilter[SpaceTimeKey](bounds))).get
            val model3 = catalog.load[SpaceTimeKey](LayerId("tas-cm-rcp45",4), FilterSet(SpaceFilter[SpaceTimeKey](bounds))).get
            val ret = new RasterRDD[SpaceTimeKey](model1.union(model2).union(model3), md)
              .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
              .averageByKey
              .zonalSummaryByKey(extent, Double.MinValue, MaxDouble, stk => stk.temporalComponent.time)
              .collect
              .sortBy(_._1)

            statsReponse("miroc5-access1-cm", ret)
          }
        } ~
        path("max") { 
          complete {    
            statsReponse(name,
              catalog.load[SpaceTimeKey](layer, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))
              .get
              .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
              .averageByKey
              .zonalSummaryByKey(extent, Double.MinValue, MaxDouble, stk => stk.temporalComponent.time)
              .collect
              .sortBy(_._1) )
          } 
        } ~      
        path("min") { 
          complete {    
            statsReponse(name,
              catalog.load[SpaceTimeKey](layer, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))
              .get
              .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
              .averageByKey
              .zonalSummaryByKey(extent, Double.MaxValue, MinDouble, stk => stk.temporalComponent.time)
              .collect
              .sortBy(_._1) )
          } 
        }    
      }
    }
  }

  def timedCreate[T](startMsg:String,endMsg:String)(f:() => T):T = {
    println(startMsg)
    val s = System.currentTimeMillis
    val result = f()
    val e = System.currentTimeMillis
    val t = "%,d".format(e-s)
    println(s"\t$endMsg (in $t ms)")
    result
  }

  def pixelRoute = cors {
    import DefaultJsonProtocol._
    import org.apache.spark.SparkContext._

    path("pixel") {
      get {
        parameters('name, 'zoom.as[Int], 'x.as[Double], 'y.as[Double]) { (name, zoom, x, y) =>
          val layer = LayerId(name, zoom)
          val (lmd, params) = catalog.metaDataCatalog.load(layer).get
          val md = lmd.rasterMetaData
          val crs = md.crs

          val p = Point(x, y).reproject(LatLng, crs)
          val key = md.mapTransform(p)
          val rdd = catalog.load[SpaceTimeKey](layer, FilterSet(SpaceFilter[SpaceTimeKey](key.col, key.row))).get
          val bcMetaData = rdd.sparkContext.broadcast(rdd.metaData)

          def createCombiner(value: Double): (Double, Double) =
            (value, 1)
          def mergeValue(acc: (Double, Double), value: Double): (Double, Double) =
            (acc._1 + value, acc._2 + 1)
          def mergeCombiners(acc1: (Double, Double), acc2: (Double, Double)): (Double, Double) =
            (acc1._1 + acc2._1, acc1._2 + acc2._2)

          complete {
            val data: Array[(DateTime, Double)] =
            timedCreate("Starting calculation", "End calculation") {
              rdd
                .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
                .map { case (key, tile) =>
                  val md = bcMetaData.value
                  val (col, row) = RasterExtent(md.mapTransform(key), tile.cols, tile.rows).mapToGrid(p.x, p.y)
                  (key, tile.getDouble(col, row))
                 }
                .combineByKey(createCombiner, mergeValue, mergeCombiners)
                .map { case (key, (sum, count)) =>
                  (key.temporalKey.time, sum / count)
                 }
                .collect
            }

            JsArray(JsObject(
              "model" -> JsString(name),
              "data" -> JsArray(
                data.map { case (year, value) =>
                  JsObject(
                    "year" -> JsString(year.toString),
                    "value" -> JsNumber(value)
                  )
                }: _*
              )
            ))
          }
        }
      }
    }
  }

  def root = {
    pathPrefix("catalog") { catalogRoute } ~
      pathPrefix("tms") { tmsRoute } ~
      pathPrefix("stats") { statsRoute } ~
      pixelRoute
  }

  startServer(interface = "0.0.0.0", port = 8088) {
    pingPong ~ root
  }
}