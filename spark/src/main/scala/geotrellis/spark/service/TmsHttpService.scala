package geotrellis.spark.service

import akka.actor._

import geotrellis._
import geotrellis.raster.render.png._    
import geotrellis.spark.cmd.TmsArgs
import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.rdd.{RasterRDD, RasterHadoopRDD, TmsPyramid}
import geotrellis.spark.tiling.{TileExtent, TmsTiling}

import org.apache.hadoop.fs.Path

import spray.http.MediaTypes
import spray.http.StatusCodes._
import spray.routing.{ExceptionHandler, HttpService}
import spray.util.LoggingContext

object TmsHttpActor {
  /**
   * One SHOULD use this method when needing Props to provide type checking for Actor constructor
   */
  def props(args: TmsArgs): Props = akka.actor.Props(classOf[TmsHttpActor], args)
}

class TmsHttpActor(val args: TmsArgs) extends Actor with TmsHttpService {
  def actorRefFactory = context

  def receive = runRoute(rootRoute)
}

trait TmsHttpService extends HttpService {
  val args: TmsArgs
  implicit val sc = args.sparkContext("TMS Service")

  def rootRoute = 
  pathPrefix("tms" / Segment / IntNumber / IntNumber / IntNumber ) { (layer, zoom, x , y) =>
    //I want some code like:
    val pyramid = new TmsPyramid(new Path(s"${args.root}/$layer"))
    val extent = TileExtent(x,y,x,y)
    val rdd = pyramid.rdd(zoom, extent) //this will return a partial rdd


    //What is the TileID that I actually want?
    val tilePng = rdd
      .filter(_.id == TmsTiling.tileId(x, y, zoom))
      .map{ t => 
        Encoder(Settings(Rgba, PaethFilter)).writeByteArray(t.tile)
       }
      .first



    //fetch the tile and return it as png
    respondWithMediaType(MediaTypes.`image/png`) { complete { 
      //rdd.map(t => TmsTiling.tileXY(t.id, zoom)).collect.mkString("\n")
      tilePng
    } }
  }
}