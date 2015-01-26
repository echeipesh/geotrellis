package geotrellis.spark.io.s3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3ObjectSummary

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.auth._
import org.apache.hadoop.mapreduce.Job

import com.typesafe.scalalogging.slf4j._

import scala.collection.JavaConversions._


abstract class S3InputFormat[K, V] extends InputFormat[K,V] with Logging {
  import S3InputFormat._

  override def getSplits(context: JobContext) = {
    val conf = context.getConfiguration
    val id = conf.get(AWS_ID)
    val key = conf.get(AWS_KEY)
    val bucket = conf.get(BUCKET)
    val prefix = conf.get(PREFIX)

    val credentials = 
      if (id != null && key != null)
        new BasicAWSCredentials(id, key)
      else      
        new DefaultAWSCredentialsProviderChain().getCredentials
    
    val s3client = new AmazonS3Client(credentials)
    
    logger.debug("Configuration: bucket=$bucket prefix=$prefix id=${credentials.getAWSAccessKeyId}, key=${credentials.getAWSSecretKey")

    val request = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(prefix)
    
    var listing: ObjectListing = null
    var splits: List[S3InputSplit] = Nil    
    do {
      listing = s3client.listObjects(request)     
      val split = new S3InputSplit()
      split.accessKeyId = credentials.getAWSAccessKeyId
      split.secretKey = credentials.getAWSSecretKey
      split.bucket = bucket
      split.keys = listing.getObjectSummaries.map(_.getKey)
    
      splits = split :: splits
      request.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)
  
    splits
  }
}

object S3InputFormat {
  final val AWS_ID = "s3.awsId"
  final val AWS_KEY = "s3.awsKey"
  final val BUCKET ="s3.bucket"
  final val PREFIX ="s3.prefix"
  final val MAX_KEYS = "s3.maxKeys" //TODO: There should be configuration for max keys per split

  def setUrl(job: Job, url: String) = {
    val conf = job.getConfiguration
    val S3Utils.rx(id, key, bucket, prefix) = url

    if (id != null && key != null) {
      conf.set(AWS_ID, id)  
      conf.set(AWS_KEY, key)  
    }
    conf.set(BUCKET, bucket)
    conf.set(PREFIX, prefix)        
  }
}