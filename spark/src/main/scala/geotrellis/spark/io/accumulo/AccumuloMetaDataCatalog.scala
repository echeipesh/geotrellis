package geotrellis.spark.io.accumulo

import geotrellis.raster.histogram.Histogram
import geotrellis.raster.io.json._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.{Key, Mutation, Value, Range => ARange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.spark.Logging
import spray.json._
import DefaultJsonProtocol._

/**
 * Accumulo Catalog Table Structure:
 *  - RowId:            $table__$layer
 *  - ColumnFamily:     zoom
 *  - ColumnQualifier:  field
 *  - Value:            field value
 */
class AccumuloMetaDataCatalog(connector: Connector, val catalogTable: String) extends MetaDataCatalog[String] with Logging {
  //create the metadata table if it does not exist
  {
    val ops = connector.tableOperations()
    if (!ops.exists(catalogTable))
      ops.create(catalogTable)
  }

  type TableName = String
  var catalog: Map[(LayerId, TableName), LayerMetaData] = fetchAll

  def zoomLevelsFor(layerName: String): Seq[Int] = {
    catalog.keys.filter(_._1.name == layerName).map(_._1.zoom).toSeq
  }

  def save(id: LayerId, table: TableName, metaData: LayerMetaData, clobber: Boolean): Unit = {
    if (catalog.contains(id -> table)) {
      // If we want to clobber, by default Accumulo will overwrite it.
      // If not, let the user know.
      if (!clobber) {
        throw new LayerExistsError(id)
      }
    }

    catalog = catalog updated ((id -> table), metaData)

    val mutation = new Mutation(s"${table}__${id.name}")
    mutation.put( //RasterMetaData
      id.zoom.toString, "metadata", System.currentTimeMillis(),
      new Value(metaData.rasterMetaData.toJson.compactPrint.getBytes)
    )
    mutation.put( //Histogram
      id.zoom.toString, "histogram", System.currentTimeMillis(),
      new Value(metaData.histogram.toJson.compactPrint.getBytes)
    )
    mutation.put( //Key ClassTag
      id.zoom.toString, "keyClass", System.currentTimeMillis(),
      new Value(metaData.keyClass.getBytes)
    )

    connector.write(catalogTable, mutation)
  }


  def load(layerId: LayerId): (LayerMetaData, TableName) = {
    val candidates = catalog
      .filterKeys( key => key._1 == layerId)

    candidates.size match {
      case 0 =>
        throw new LayerNotFoundError(layerId)
      case 1 =>
        val (key, value) = candidates.toList.head
        (value, key._2)
      case _ =>
        throw new MultipleMatchError(layerId)
    }
  }

  def load(layerId: LayerId, table: String): LayerMetaData =
    catalog.get(layerId -> table) match {
      case Some(md) => md
      case None =>
        throw new LayerNotFoundError(layerId)
    }

  def fetchAll: Map[(LayerId, TableName), LayerMetaData] = {
    var data: Map[(LayerId, TableName), Map[String, Value]] =
      Map.empty.withDefaultValue(Map.empty)

    connector.createScanner(catalogTable, new Authorizations()).foreach { case (key, value) =>
      val Array(table, name) = key.getRow.toString.split("__")
      val zoom: Int = key.getColumnFamily.toString.toInt
      val layerId = LayerId(name, zoom)
      val field = key.getColumnQualifier.toString

      val k = layerId -> table
      data = data updated (k, data(k) updated (field, value))
    }

    def readLayerMetaData(map: Map[String, Value]): LayerMetaData =
      LayerMetaData(
        keyClass =  map("keyClass").toString,
        rasterMetaData = map("metadata").toString.parseJson.convertTo[RasterMetaData],
        histogram = map.get("histogram").map(_.toString.parseJson.convertTo[Histogram])
      )

    data map { case (key, fieldMap) => key -> readLayerMetaData(fieldMap)}
  }
}
