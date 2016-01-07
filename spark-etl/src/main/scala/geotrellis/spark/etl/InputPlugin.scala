package geotrellis.spark.etl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait Plugin {
  def requiredKeys: Array[String]
  type Parameters = Map[String, String]
}

trait InputPlugin[I, V] extends Plugin with Serializable {
  def name: String
  def format: String

  def validate(props: Map[String, String]) =
    requireKeys(name, props, requiredKeys)

  def suitableFor(name: String, format: String): Boolean =
    (name.toLowerCase, format.toLowerCase) == (this.name, this.format)

  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(I, V)]
}