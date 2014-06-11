package geotrellis.spark.service

import akka.actor._
import spray.util.LoggingContext
import spray.routing.ExceptionHandler
import spray.http.StatusCodes._

import spray.routing.{ExceptionHandler, HttpService}
import spray.http.MediaTypes
import spray.http.StatusCodes.InternalServerError
import spray.util.LoggingContext

import geotrellis._
import geotrellis.source.{ValueSource, RasterSource}
import geotrellis.process.{Error, Complete}
import geotrellis.render.ColorRamps
import geotrellis.statistics.Histogram
import geotrellis.render.png.Renderer

import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.rdd.RasterRDD
import geotrellis.spark.tiling.TmsTiling

import geotrellis.spark.cmd.TmsArgs
import org.apache.hadoop.fs.Path

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
  val sc = args.sparkContext("TMS Service")

  def RenderRasterToPng(r: Raster): Array[Byte] = {
    import geotrellis.render.png._    

    Encoder(Settings(Rgba, PaethFilter)).writeByteArray(r)
  }

  def rootRoute = 
  pathPrefix("tms" / Segment / IntNumber / IntNumber / IntNumber ) { (layer, zoom, x , y) =>
    //TODO - refactor this out to utility, maybe there is a Pyramid Raster? I don't know
    //val reader = RasterReader(s"${args.inputraster}/$layer/$zoom", args.hadoopConf)
    val pyramidPath = new Path(s"${args.root}/$layer")    
    val meta = PyramidMetadata(pyramidPath, args.hadoopConf) //TODO - refactor hadoop conf out to implicit
    val rdd = RasterRDD(s"$pyramidPath/$zoom", sc)

    //What is the TileID that I actually want?
    val tilePng = rdd
      .filter(_.id == TmsTiling.tileId(x, y, zoom))
      .map(t => RenderRasterToPng(t.raster))
      .first



    //fetch the tile and return it as png
    respondWithMediaType(MediaTypes.`image/png`) { complete { 
      //rdd.map(t => TmsTiling.tileXY(t.id, zoom)).collect.mkString("\n")
      tilePng
    } }
  }
}