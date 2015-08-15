package geotrellis.spark.etl.accumulo

import com.google.inject.AbstractModule
import com.google.inject.multibindings.Multibinder
import geotrellis.spark.SpatialKey
import geotrellis.spark.etl.hadoop.HadoopModule._
import geotrellis.spark.etl.hadoop.{SpatialHadoopOutput, GeoTiffHadoopInput}
import geotrellis.spark.etl.{OutputPlugin, InputPlugin}

object AccumuloModule extends AbstractModule {
  override def configure() {
    val outputBinder = Multibinder.newSetBinder(binder(), classOf[OutputPlugin])
    outputBinder.addBinding().to(classOf[SpatialAccumuloOutput])
  }
}