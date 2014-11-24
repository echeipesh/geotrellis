package geotrellis.spark.ingest

import geotrellis.spark._
import geotrellis.spark.cmd.args.{ SparkArgs, HadoopArgs, AccumuloArgs }
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken

import org.apache.hadoop.fs._

import org.apache.spark._

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required

import scala.reflect.ClassTag

class AccumuloPyramidArgs extends AccumuloArgs with SparkArgs with HadoopArgs {
  @Required var layerName: String = _
  @Required var table: String = _
  @Required var startLevel: Int = _
}

object AccumuloPyramidCommand extends ArgMain[AccumuloPyramidArgs] with Logging {
  def main(args: AccumuloPyramidArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = args.sparkContext("Ingest")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    val rdd = catalog.load[SpatialKey](LayerId(args.layerName, args.startLevel)).get

    val layoutScheme = ZoomedLayoutScheme(256)
    val level = layoutScheme.levelFor(args.startLevel)

    val save = { (rdd: RasterRDD[SpatialKey], level: LayoutLevel) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, true)
    }

    Pyramid.saveLevels(rdd, level, layoutScheme)(save)
  }
}