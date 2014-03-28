package geotrellis.feature.json

import spray.json._
import geotrellis.feature._
import scala.collection.generic.CanBuildFrom

/**
 * This is a container for T which is able to read/write GeoJSON for value T
 *
 * The idea is that GeoReader[Format[G,D] ] is good for GeoReader[PointFeature[D] ]
 * In fact, there will be no reader for GeoFeature[D]
 *
 */
trait GeoReader[-T] { def read[J <: T](value: JsValue): J }
trait GeoWriter[-T]{ def write(g: T): JsValue }
trait GeoFormat[-T] extends GeoReader[T] with GeoWriter[T]


trait SomeImplicits {

  //TODO this can really just copy/paste the stuff in here
  implicit object GeometryGeoFormat extends GeoFormat[Geometry] {
    override def read[J](value: JsValue): J =
      GeometryFormats.GeometryFormat.read(value).asInstanceOf[J]
    override def write(g: Geometry): JsValue =
      GeometryFormats.GeometryFormat.write(g)
  }

  /*
  So the really not-great thing about all this is that it's not possible to be specific
  about the type of collection you want, unless there is some sort of CanBuildFrom interface
   */
  implicit object GeometryCollectionGeoFormat extends GeoFormat[Seq[Geometry]] {
    override def write(g: Seq[Geometry]): JsValue =
      GeometryFormats.GeometryCollectionFormat.write(g)
    override def read[J <: Seq[Geometry]](value: JsValue): J = {
      //TODO: THIS IS VERY VERY BAD, BUT DEPENDS ON HOW YOU WANT TO SOLVE THIS
      GeometryFormats.GeometryCollectionFormat.read(value).asInstanceOf[J]
    }
  }

  implicit def FeatureGeoFormat[G <:Geometry, D: JsonFormat] = new FeatureGeoFormat[G, D]
  class FeatureGeoFormat[G<: Geometry , D: JsonFormat] extends GeoFormat[Feature[G, D]] {
    override def write(obj: Feature[G, D]): JsValue =
      JsObject(
        "type" -> JsString("Feature"),
        "geometry" -> GeometryGeoFormat.write(obj.geom),
        "properties" -> obj.data.toJson
      )

    override def read[J <: Feature[G, D]](value: JsValue): J = {
      value.asJsObject.getFields("type", "geometry", "properties") match {
        case Seq(JsString("Feature"), geom, data) =>
          val g = GeometryGeoFormat.read[G](geom)
          val d = data.convertTo[D]

          {g match {
            case g:Point => PointFeature(g, d)
            case g:Line => LineFeature(g, d)
          }}.asInstanceOf[J]

        case _ => throw new DeserializationException("Feature expected")
      }
    }
  }


  implicit def FeatureCollectionGeoFormat[G <:Geometry, D: JsonFormat] = new FeatureCollectionGeoFormat[G, D]
  class FeatureCollectionGeoFormat[G <:Geometry, D: JsonFormat] extends GeoFormat[Seq[Feature[G,D]]]{
    override def write(fc: Seq[Feature[G, D]]): JsValue =  JsObject(
      "type" -> JsString("FeatureCollection"),
      "features" -> JsArray(fc.map { f => FeatureGeoFormat.write(f) }.toList)
    )

    override def read[J <: Seq[Feature[G, D]]](value: JsValue): J =
      value.asJsObject.getFields("features") match {
        case Seq(JsArray(features)) =>
        {for (feature <- features) yield
            FeatureGeoFormat[G,D].read[Feature[G,D]](feature)
        }.toVector.asInstanceOf[J]
      }
  }


}
object SomeImplicits extends SomeImplicits