package geotrellis.spark.io.accumulo

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.data.{Range => ARange, Key, Value, Mutation}
import org.apache.accumulo.core.util.{Pair => APair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import scala.collection.mutable

object TimeRasterAccumuloDriver extends AccumuloDriver[SpaceTimeKey] {
  def rowId(layerId: LayerId, col: Int, row: Int, time: String) = {
    new Text(f"${layerId.zoom}%02d_${col}%06d_${row}%06d_${time}")
  }
  val rowIdRx = """(\d+)_(\d+)_(\d+)_(\d+)-(\d+)""".r 

  /** Map rdd of indexed tiles to tuples of (table name, row mutation) */
  def encode(layerId: LayerId, raster: RasterRDD[SpaceTimeKey]): RDD[(Text, Mutation)] =
    raster.map {
      case (SpaceTimeKey(spatialKey, time), tile) =>        
        val mutation = new Mutation(rowId(layerId, spatialKey.col, spatialKey.row, time.time.toString.substring(0,7)))
        mutation.put(new Text(layerId.name), new Text(time.withZone(DateTimeZone.UTC).toString),
          System.currentTimeMillis(), new Value(tile.toBytes()))
        (null, mutation)
    }

  /** Maps RDD of Accumulo specific Key, Value pairs to a tuple of (K, Tile) and wraps it in RasterRDD */
  def decode(rdd: RDD[(Key, Value)], metaData: RasterMetaData): RasterRDD[SpaceTimeKey] = {
    val tileRdd = rdd.map {
      case (key, value) =>
        val rowIdRx(zoom, col, row, _, _) = key.getRow.toString
        val spatialKey = SpatialKey(col.toInt, row.toInt)
        val time = DateTime.parse(key.getColumnQualifier.toString)
        val tile = ArrayTile.fromBytes(value.get, metaData.cellType, metaData.tileLayout.tileCols, metaData.tileLayout.tileRows)
        SpaceTimeKey(spatialKey, time) -> tile.asInstanceOf[Tile]
    }
    new RasterRDD(tileRdd, metaData)
  }

  def setZoomBounds(job: Job, layerId: LayerId): Unit = {
    val range = new ARange(
      new Text(f"${layerId.zoom}%02d"),
      new Text(f"${layerId.zoom+1}%02d")
    ) :: Nil
    InputFormatBase.setRanges(job, range)
  }

  def tileSlugs(filters: List[GridBounds]): List[(String, String)] = filters match {
    case Nil =>
      List(("0"*6 + "_" + "0"*6) -> ("9"*6 + "_" + "9"*6))
    case _ => 
      for{
        bounds <- filters
        row <- bounds.rowMin to bounds.rowMax 
      } yield f"${bounds.colMin}%06d_${row}%06d" -> f"${bounds.colMax}%06d_${row}%06d"      
  }
  
  def timeSlugs(filters: List[(DateTime, DateTime)]): List[(String, String)] = filters match {
    case Nil =>
      List(("0"*4 + "-" + "0"*2) -> ("9"*4 + "-" + "9"*2))
    case List((start, end)) =>                 
      List(start.toString.substring(0,7) -> end.toString.substring(0,7))
  }

  def setFilters(job: Job, layerId: LayerId, filterSet: FilterSet[SpaceTimeKey]): Unit = {
    var spaceFilters: List[GridBounds] = Nil
    var timeFilters: List[(DateTime, DateTime)] = Nil

    filterSet.filters.foreach {
      case SpaceFilter(bounds) => 
        spaceFilters = bounds :: spaceFilters
      case TimeFilter(start, end) =>
        timeFilters = (start, end) :: timeFilters
    }

    val ranges = for {
      (tileFrom, tileTo) <- tileSlugs(spaceFilters)
      (timeFrom, timeTo) <- timeSlugs(timeFilters)
    } yield {
      val from = f"${layerId.zoom}%02d_${tileFrom}_${timeFrom}"
      val to = f"${layerId.zoom}%02d_${tileTo}_${timeTo}"
      println("FILTER", from, to)
      new ARange(from, to)
    }

    InputFormatBase.setRanges(job, ranges)

    assert(timeFilters.length == 0, "Only one TimeFilter supported at this time")

    for ( (start, end) <- timeFilters) {
      val props =  Map(
        "startBound" -> start.toString,
        "endBound" -> end.toString,
        "startInclusive" -> "true",
        "endInclusive" -> "true"
      )

      InputFormatBase.addIterator(job, 
        new IteratorSetting(1, "TimeColumnFilter", "org.apache.accumulo.core.iterators.user.ColumnSliceFilter", props))
    }

    InputFormatBase.fetchColumns(job, new APair(new Text(layerId.name), null: Text) :: Nil)
  }
}

// 02_000000_000001_0000-00
// 02_000000_000001_2026-06
// 02_000001_000001_9999-99
