package geotrellis.spark.io.accumulo

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.impl.{Tables, TabletLocator}
import org.apache.accumulo.core.client.mapreduce.lib.util.{InputConfigurator => IC, ConfiguratorBase => CB}
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.data.{Range => ARange, _}
import org.apache.accumulo.core.master.state.tables.TableState
import org.apache.accumulo.core.security.thrift.TCredentials
import org.apache.accumulo.core.security.{CredentialHelper, Authorizations}
import org.apache.accumulo.core.util.UtilWaitThread
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, JobContext}
import org.apache.log4j.Level
import scala.collection.JavaConversions._


/**
 * It is not clear what would be better, a series of scanners or a BatchScanner.
 * BatchScanner is way easier to code for this, so that's what's happening.
 */
class MultiRangeRecordReader extends RecordReader[Key, Value] {
  var scanner: BatchScanner = null
  var iterator: Iterator[java.util.Map.Entry[Key, Value]] = null
  var key: Key = null
  var value: Value = null

  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val queryThreads = 1
    val sp = split.asInstanceOf[MultiRangeInputSplit]
    val connector = sp.connector
    scanner = connector.createBatchScanner(sp.table, new Authorizations(), queryThreads)
    scanner.setRanges(sp.ranges)
    iterator = scanner.iterator()
  }

  def getProgress: Float = ???

  def nextKeyValue(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext) {
      val entry = iterator.next()
      key = entry.getKey
      value = entry.getValue
    }
    hasNext
  }

  def getCurrentValue: Value = value

  def getCurrentKey: Key = key

  def close(): Unit = {
    scanner.close
  }
}