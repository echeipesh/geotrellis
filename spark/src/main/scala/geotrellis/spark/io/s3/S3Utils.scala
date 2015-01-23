package geotrellis.spark.io.s3

import scala.util.matching.Regex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.BasicAWSCredentials

import scala.collection.JavaConversions._

object S3Utils {
  private val idRx = "[A-Z0-9]{20}"
  private val keyRx = "[a-zA-Z0-9+/]+={0,2}"
  private val slug = "[a-z0-9-]+"
  
  val rx = new Regex(s"""s3n://(?:($idRx):($keyRx)@)?($slug)/{0,1}(.*)""", 
    "aws_id", "aws_key", "bucket", "prefix")

  def listFiles(url: String): List[Path] = {
    val rx(id,key,bucket,prefix) = url
    var files: List[Path] = Nil

    val s3client = 
      if (id != null && key != null)
        new AmazonS3Client(new BasicAWSCredentials(id, key))
      else
        new AmazonS3Client(new ProfileCredentialsProvider())

    def toPath(path: String) = 
      if (id != null && key != null)
        new Path(s"""s3n://$id:$key@$bucket/$path""")
      else
        new Path(s"""s3n://$bucket/$prefix""")

    val request = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(prefix)
    
    var listing: ObjectListing = null
    do {
      listing = s3client.listObjects(request)
      for (summary <- listing.getObjectSummaries)
        files = toPath(summary.getKey) :: files

      request.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    files
  }

  def listFiles(path: Path): List[Path] = 
    listFiles(path.toUri.toString)
}