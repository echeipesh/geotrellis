package geotrellis.spark.io.accumulo

import java.io.{IOException, DataOutput, DataInput}
import java.nio.charset.Charset
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
import org.apache.accumulo.core.util.{Pair => APair, UtilWaitThread}
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.{Writable, Text}
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, JobContext}
import org.apache.log4j.Level
import scala.collection.JavaConversions._

class MultiRangeInputSplit extends InputSplit with Writable {
  var ranges: Seq[ARange] = null
  var location: String = null
  var table: String = null
  var instanceName: String = null
  var zooKeepers: String = null
  var principal: String = null
  var token: AuthenticationToken = null
  var fetchedColumns: java.util.Set[APair[Text, Text]] = null

  val offline: Boolean = false
  var mockInstance: Boolean = false
  val isolatedScan: Boolean = false
  var localIterators: Boolean = false
  var auths: Authorizations = null
  var iterators: List[IteratorSetting] = Nil
  var level: Level = Level.DEBUG

  def instance = new ZooKeeperInstance(instanceName, zooKeepers)

  def connector = instance.getConnector(principal, token)

  override def getLength: Long = ranges.length


  /** By definition this split can have only one location */
  override def getLocations: Array[String] = Array(location)


  override def write(out: DataOutput): Unit = {
    out.writeInt(ranges.length)
    ranges foreach { range => range .write(out) }
    out.writeBoolean(null != table)
    if (null != table) {
      out.writeUTF(table)
    }
    out.writeUTF(location)
    out.writeBoolean(localIterators)
    if (localIterators) {
      out.writeBoolean(localIterators)
    }
    out.writeBoolean(mockInstance)
    if (mockInstance) {
      out.writeBoolean(mockInstance)
    }
    out.writeBoolean(null != fetchedColumns)
    if (null != fetchedColumns) {
      val cols: Array[String] = IC.serializeColumns(fetchedColumns)
      out.writeInt(cols.length)
      for (col <- cols) {
        out.writeUTF(col)
      }
    }
    out.writeBoolean(null != auths)
    if (null != auths) {
      out.writeUTF(auths.serialize)
    }
    out.writeBoolean(null != principal)
    if (null != principal) {
      out.writeUTF(principal)
    }
    out.writeBoolean(null != token)
    if (null != token) {
      out.writeUTF(token.getClass.getCanonicalName)
      try {
        out.writeUTF(CredentialHelper.tokenAsBase64(token))
      }
      catch {
        case e: AccumuloSecurityException => {
          throw new IOException(e)
        }
      }
    }
    out.writeBoolean(null != instanceName)
    if (null != instanceName) {
      out.writeUTF(instanceName)
    }
    out.writeBoolean(null != zooKeepers)
    if (null != zooKeepers) {
      out.writeUTF(zooKeepers)
    }
    out.writeBoolean(null != iterators)
    if (null != iterators) {
      out.writeInt(iterators.size)
      import scala.collection.JavaConversions._
      for (iterator <- iterators) {
        iterator.write(out)
      }
    }
    out.writeBoolean(null != level)
    if (null != level) {
      out.writeInt(level.toInt)
    }
  }

  override def readFields(in: DataInput): Unit = {
    val numRanges = in.readInt()
    ranges = for(i <- 0  until numRanges) yield {
      val range = new ARange()
      range.readFields(in)
      range
    }
    if (in.readBoolean()) {
      table = in.readUTF()
    }    
    location = in.readUTF()

    if (in.readBoolean()) {
      localIterators = in.readBoolean()
    }

    if (in.readBoolean()) {
      mockInstance = in.readBoolean()
    }

    if (in.readBoolean()) {
      val numColumns = in.readInt()
      var columns: List[String] = Nil
      for (i <- 0 until numColumns) {
        columns = in.readUTF() :: columns
      }
      fetchedColumns = IC.deserializeFetchedColumns(columns)
    }

    if (in.readBoolean()) {
      val strAuths = in.readUTF()
      auths = new Authorizations(strAuths.getBytes(Charset.forName("UTF-8")))
    }

    if (in.readBoolean()) {
      principal = in.readUTF();
    }

    if (in.readBoolean()) {
      val tokenClass = in.readUTF();
      val base64TokenBytes = in.readUTF().getBytes(Charset.forName("UTF-8"));
      val tokenBytes = Base64.decodeBase64(base64TokenBytes);
      token = CredentialHelper.extractToken(tokenClass, tokenBytes);
    }

    if (in.readBoolean()) {
      instanceName = in.readUTF();
    }

    if (in.readBoolean()) {
      zooKeepers = in.readUTF();
    }

    if (in.readBoolean()) {
      val numIterators = in.readInt()      
      for (i <- 0 until numIterators) {
        iterators = new IteratorSetting(in) :: iterators
      }
    }

    if (in.readBoolean()) {
      level = Level.toLevel(in.readInt());
    }
  }
}
