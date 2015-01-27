package geotrellis.spark.io.s3

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import com.typesafe.scalalogging.slf4j.Logging
import java.io.{InputStream, ByteArrayOutputStream}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}

abstract class S3RecordReader[K, V] extends RecordReader[K, V] with Logging {
  var bucket: String = _
  var s3client: AmazonS3Client = _
  var keys: Iterator[String] = null
  var curKey: K = _
  var curValue: V = _
  var keyCount: Int = _
  var curCount: Int = 0

  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val sp = split.asInstanceOf[S3InputSplit]
    s3client = new AmazonS3Client(sp.credentials)
    keys = sp.keys.iterator
    keyCount =  sp.keys.length
    bucket = sp.bucket
    logger.info(s"Initialize split bucket '$bucket' with $keyCount keys")
  }

  def getProgress: Float = curCount / keyCount

  def read(obj: Array[Byte]): (K, V)

  def nextKeyValue(): Boolean = {
    if (keys.hasNext){
      val key = keys.next
      logger.info(s"Reading key: $key")
      val obj = s3client.getObject(new GetObjectRequest(bucket, key))
      val inStream = obj.getObjectContent
      val objectData = S3RecordReader.readInputStream(inStream)
      inStream.close
      
      val (k, v) = read(objectData)          
      curKey = k
      curValue = v
      curCount += 1      
      true
    } else {
      false
    }
  }

  def getCurrentKey: K = curKey

  def getCurrentValue: V = curValue

  def close(): Unit = {}
}

object S3RecordReader {
    def readInputStream(inStream: InputStream): Array[Byte] = {
    val bufferSize = 0x20000
    val buffer = new Array[Byte](bufferSize)
    val outStream = new ByteArrayOutputStream(bufferSize)    
    var bytes: Int = 0
    while (bytes != -1) {      
      bytes = inStream.read(buffer)
      if (bytes != -1) outStream.write(buffer, 0, bytes);
    }
    outStream.toByteArray
  }
}