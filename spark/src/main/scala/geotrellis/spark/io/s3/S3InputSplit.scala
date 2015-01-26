package geotrellis.spark.io.s3

import java.io.{DataOutput, DataInput}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}

class S3InputSplit extends InputSplit with Writable 
{
  var accessKeyId: String = _
  var secretKey: String = _
  var bucket: String = _
  var keys: Seq[String] = Seq.empty

  def credentials: AWSCredentials = new BasicAWSCredentials(accessKeyId, secretKey)

  override def getLength: Long = keys.length

  override def getLocations: Array[String] = Array.empty

  override def write(out: DataOutput): Unit = {
    out.writeUTF(accessKeyId)
    out.writeUTF(secretKey)
    out.writeUTF(bucket)
    out.writeInt(keys.length)
    keys.foreach(out.writeUTF)
  }
  
  override def readFields(in: DataInput): Unit = {
    accessKeyId = in.readUTF
    secretKey = in.readUTF
    bucket = in.readUTF
    val keyCount = in.readInt
    keys = for (i <- 1 to keyCount) yield in.readUTF
  }
}