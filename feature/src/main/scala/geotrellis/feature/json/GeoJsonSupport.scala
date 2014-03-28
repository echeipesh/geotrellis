package geotrellis.feature.json

import geotrellis.feature._

import spray.json._
import spray.http._
import spray.httpx._
import spray.httpx.marshalling.Marshaller
import spray.httpx.unmarshalling.Unmarshaller

/**
 * Decisions that are queued:
 *
 * - geotrellis.feature.GeometryCollection decomposes into Point, Lines, Polygon, Why does it do that?
 *  - Continue implicitly wrap Seq[Geo*] or Make it a fullfledged class. I'd prefer the latter
 * - Should Geometry readers/writers still extend RootJsonFormat
 *  - They can be used independently of GeoJsonSupoort I guess, but that doesn't seem like something I actually want to allow
 *    - GeometryFormats can be changed to be flat out functions if GeometryFormat is going to be the only interface
 */


/**
 * A trait providing automatic to and from JSON marshalling/unmarshalling using an in-scope *spray-json* protocol.
 * Note that *spray-httpx* does not have an automatic dependency on *spray-json*.
 * You'll need to provide the appropriate *spray-json* artifacts yourself.
 */
trait GeoJsonSupport extends GeometryFormats {


  implicit def sprayJsonUnmarshaller[T :GeoReader] =
    Unmarshaller[T](MediaTypes.`application/json`) {
      case x: HttpEntity.NonEmpty ⇒
        val json = JsonParser(x.asString(defaultCharset = HttpCharsets.`UTF-8`))
        val reader = implicitly[GeoReader[T]]
        reader.read(json)
    }

  implicit def sprayJsonMarshaller[T :GeoWriter](implicit
    printer: JsonPrinter = PrettyPrinter,
    crs: CRS = GeoJsonSupport.defaultCrs
  ) = Marshaller.delegate[T, String](ContentTypes.`application/json`) { value =>
      val writer = implicitly[GeoWriter[T]]
      val json = writer.write(value)
      printer(crs.addTo(json))
  }
}

object GeoJsonSupport extends GeoJsonSupport{
  implicit val defaultCrs = BlankCRS
}

