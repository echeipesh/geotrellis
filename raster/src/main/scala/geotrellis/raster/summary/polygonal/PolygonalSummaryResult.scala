package geotrellis.raster.summary.polygonal

sealed trait PolygonalSummaryResult[+A]

case object NoIntersection extends PolygonalSummaryResult[Nothing]
case class Summary[A](value: A) extends PolygonalSummaryResult[A]

// TODO: Add Monad on PolygonalSummaryResult companion object to support flatMaps
