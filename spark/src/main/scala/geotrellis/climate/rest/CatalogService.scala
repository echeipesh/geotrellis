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
import geotrellis.spark.json._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector.reproject._

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

trait CORSSupport { self: HttpService =>
  val corsHeaders = List(`Access-Control-Allow-Origin`(AllOrigins),
    `Access-Control-Allow-Methods`(GET, POST, OPTIONS, DELETE),
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"))

  def cors: Directive0 = {
    val rh = implicitly[RejectionHandler]
    respondWithHeaders(corsHeaders) & handleRejections(rh)
  }
}

/**
 * Catalog and TMS service for TimeRaster layers only
 * This is intended to exercise the machinery more than being a serious service.
 */
object CatalogService extends ArgApp[AccumuloArgs] with SimpleRoutingApp with SprayJsonSupport with CORSSupport {
  implicit val system = ActorSystem("spray-system")
  implicit val sparkContext = SparkUtils.createSparkContext("Catalog")

  val accumulo = AccumuloInstance(argHolder.instance, argHolder.zookeeper,
    argHolder.user, new PasswordToken(argHolder.password))
  val catalog = accumulo.catalog

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

          accumulo.metaDataCatalog.fetchAll.mapValues(_._1).toSeq.map {
            case (layer, md) =>                          
              val center = md.extent.reproject(md.crs, LatLng).center
              val breaks = {
                (if (layer.name == "NLCD")
                  Histogram(catalog.load[SpatialKey](layer).get)
                else
                  Histogram(catalog.load[SpaceTimeKey](layer).get)
                ).getQuantileBreaks(12)
              }
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
      val (md, params) = accumulo.metaDataCatalog.load(layer).get

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
  def root = {
    pathPrefix("catalog") { catalogRoute } ~
      pathPrefix("tms") { tmsRoute }
  }

  startServer(interface = "localhost", port = 8080) {
    get(pingPong ~ root)
  }
}