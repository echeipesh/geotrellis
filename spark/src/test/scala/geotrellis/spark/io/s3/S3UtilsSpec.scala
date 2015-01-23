package geotrellis.spark.io.s3

import geotrellis.spark._
import geotrellis.spark.ingest._
import geotrellis.raster._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.tiling._
import geotrellis.proj4.LatLng
import geotrellis.spark.utils.SparkUtils

import org.apache.hadoop.fs.Path
import org.joda.time.DateTime
import org.scalatest._

import org.jets3t.service._
import org.jets3t.service.security._
import org.jets3t.service.impl.rest.httpclient.RestS3Service

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

class S3UtilsSpec extends FunSpec
  with Matchers
{
  describe("S3Utils") {    
    it("should parse the s3 url containing keys") {
      val url = "s3n://AAIKJLIB4YGGVMAATT4A:ZcjWmdXN+75555bptjE4444TqxDY3ESZgeJxGsj8@nex-bcsd-tiled-geotiff/prefix/subfolder"      
      val S3Utils.rx(id,key,bucket,prefix) = url
      
      id should be ("AAIKJLIB4YGGVMAATT4A")
      key should be ("ZcjWmdXN+75555bptjE4444TqxDY3ESZgeJxGsj8")
      bucket should be ("nex-bcsd-tiled-geotiff")
      prefix should be ("prefix/subfolder")      
    }

    it("should parse s3 url without keys"){
      val url = "s3n://nex-bcsd-tiled-geotiff/prefix/subfolder"      
      val S3Utils.rx(id,key,bucket,prefix) = url
      
      id should be (null)
      key should be (null)
      bucket should be ("nex-bcsd-tiled-geotiff")
      prefix should be ("prefix/subfolder")  
    }
  }
}