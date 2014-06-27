package geotrellis.spark.service

import akka.actor._

import geotrellis._
import geotrellis.raster.render.png._    
import geotrellis.spark.cmd.TmsArgs
import geotrellis.spark.metadata.PyramidMetadata
import geotrellis.spark.rdd.{RasterRDD, RasterHadoopRDD, TmsPyramid, BufferRDD}
import geotrellis.spark.tiling.{TileExtent, TmsTiling}

import org.apache.hadoop.fs.Path

import scalaz.Memo._

import spray.http.MediaTypes
import spray.http.StatusCodes._
import spray.routing.{ExceptionHandler, HttpService}
import spray.util.LoggingContext

import scala.collection.mutable

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

  val pyramids: (String => TmsPyramid) = immutableHashMapMemo{ layer => 
    new TmsPyramid(new Path(s"${args.root}/$layer"))
  }
 
  //This cache is local to an Actor 
  // TODO: This should be it's own actor, for certain
  val cache = mutable.HashMap.empty[(String, Int), BufferRDD]
  def getBuffer(layer: String, zoom: Int, x: Int, y: Int): RasterRDD = {
    cache.get((layer, zoom)) match {
      case None => 
        val buffer = pyramids(layer).getBuffer(zoom, x, y, 5)
        cache.update(layer -> zoom, buffer)
        buffer.rdd
      
      case Some(BufferRDD(rdd, extent)) if !extent.contains(x, y) => 
        cache.remove(layer -> zoom) // TODO: Copy-Paste, this smells
        val buffer = pyramids(layer).getBuffer(zoom, x, y, 5)
        cache.update(layer -> zoom, buffer)
        buffer.rdd

      case Some(BufferRDD(rdd, extent)) if extent.contains(x, y) => 
        rdd
    }
  }


  def rootRoute = 
  pathPrefix("tms" / Segment / IntNumber / IntNumber / IntNumber ) { (layer, zoom, x , y) =>

    //This is ok, because hopefully we already had an extent in our cache    
    val rdd = getBuffer(layer, zoom, x, y)

    //How do I get a tile from the RDD?
    val tilePng = rdd
      .filter(_.id == TmsTiling.tileId(x, y, zoom))
      .map{ t => Encoder(Settings(Rgba, PaethFilter)).writeByteArray(t.tile) }
      .first

    //fetch the tile and return it as png
    respondWithMediaType(MediaTypes.`image/png`) { complete { 
      tilePng
    } }
  }
}