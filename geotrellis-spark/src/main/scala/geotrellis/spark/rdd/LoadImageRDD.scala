package geotrellis.spark.rdd

import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import geotrellis.spark.formats.ArgWritable
import geotrellis.spark.formats.TileIdWritable
import geotrellis.spark.utils.GeotrellisSparkUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.HadoopRDD
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.FileInputFormat

class LoadImageRDD(
  sc: SparkContext,
  path: String,
  broadcastedConf: Broadcast[SerializableWritable[Configuration]],
  minSplits: Int)
  extends HadoopRDD[TileIdWritable, ArgWritable](
		  	sc,
		  	broadcastedConf,
		  	Some((jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)),
		  	classOf[SequenceFileInputFormat[TileIdWritable, ArgWritable]],
		  	classOf[TileIdWritable],
		  	classOf[ArgWritable],
		  	minSplits) {
  
  /*
   * Overriding the partitioner with a TileIdPartitioner 
   */
  override val partitioner = {
    val splitFile = path.stripSuffix(LoadImageRDD.SeqFileGlob) + Path.SEPARATOR + TileIdPartitioner.SplitFile
    Some(TileIdPartitioner(splitFile, sc.hadoopConfiguration))
  }
}

object LoadImageRDD {

  val SeqFileGlob = "/*[0-9]*/data"

  def apply(sc: SparkContext, path: String) = {
    val globbedPath = path + SeqFileGlob

    new LoadImageRDD(
      sc, globbedPath, sc.broadcast(new SerializableWritable(sc.hadoopConfiguration)), sc.defaultMinSplits)
  }
}