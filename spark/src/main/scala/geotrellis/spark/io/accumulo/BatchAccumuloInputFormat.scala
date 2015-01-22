package geotrellis.spark.io.accumulo

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.accumulo.core.client.impl.{Tables, TabletLocator}
import org.apache.accumulo.core.client.mapreduce.lib.util.{InputConfigurator => IC, ConfiguratorBase => CB}
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.client.{IteratorSetting, TableOfflineException, TableDeletedException, Instance}
import org.apache.accumulo.core.data.{Range => ARange, Value, Key, KeyExtent}
import org.apache.accumulo.core.master.state.tables.TableState
import org.apache.accumulo.core.security.thrift.TCredentials
import org.apache.accumulo.core.security.{CredentialHelper, Authorizations}
import org.apache.accumulo.core.util.UtilWaitThread
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, JobContext}
import org.apache.log4j.Level
import scala.collection.JavaConversions._

class BatchAccumuloInputFormat extends InputFormatBase[Key, Value] {
  /** We're going to lie about our class so we can re-use Accumulo InputConfigurator to pull our Job settings */
  private val CLASS: Class[_] = classOf[AccumuloInputFormat]

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = context.getConfiguration

    val ranges  = IC.getRanges(CLASS, conf)
    val tableName = IC.getInputTableName(CLASS, conf)
    val instance = CB.getInstance(CLASS, conf)
    val tabletLocator = IC.getTabletLocator(CLASS, conf)
    val tokenClass = CB.getTokenClass(CLASS, conf)
    val principal = CB.getPrincipal(CLASS, conf)
    val tokenBytes = CB.getToken(CLASS, conf)
    val token = CredentialHelper.extractToken(tokenClass, tokenBytes)
    val credentials = new TCredentials(principal, tokenClass, ByteBuffer.wrap(tokenBytes), instance.getInstanceID)

    /** Ranges binned by tablets */
    val binnedRanges = new java.util.HashMap[String, java.util.Map[KeyExtent, java.util.List[ARange]]]()

    // loop until list of tablet lookup failures is empty
    while (! tabletLocator.binRanges(ranges, binnedRanges, credentials).isEmpty) {
      var tableId: String = null
      if (! instance.isInstanceOf[MockInstance]) {
        if (tableId == null)
          tableId = Tables.getTableId(instance, tableName)
        if (! Tables.exists(instance, tableId))
          throw new TableDeletedException(tableId)
        if (Tables.getTableState(instance, tableId) eq TableState.OFFLINE)
          throw new TableOfflineException(instance, tableId)
      }
      binnedRanges.clear()
      //logger.warn("Unable to locate bins for specified ranges. Retrying.")
      UtilWaitThread.sleep(100 + (Math.random * 100).toInt)
      tabletLocator.invalidateCache()
    }
    // location: String = server:ip for the tablet server
    // list: Map[KeyExtent, List[ARange]]
    binnedRanges map { case (location, list) =>
      list.map { case (keyExtent, extentRanges) =>        
        val tabletRange = keyExtent.toDataRange        
        val split = new MultiRangeInputSplit()
        split.ranges = extentRanges map { tabletRange.clip }
        split.location = location
        split.table = tableName
        split.instanceName = instance.getInstanceName
        split.zooKeepers = instance.getZooKeepers
        split.principal = principal
        split.token = token
        split.fetchedColumns = IC.getFetchedColumns(CLASS, conf)
        split
      }
    }
  }.flatten.toList

  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Key, Value] = {
    val reader = new MultiRangeRecordReader()
    reader.initialize(inputSplit, taskAttemptContext)
    reader
  }
}



