package telemetry.parquet

/*
 [JAR Hell, Abandon hope all ye who enter here]
 As Spark 1.5 uses an older version of AvroParquetWriter, we have to load one that supports getDataSize, while not shadowing the one used by Spark at the same time.
 */

import com.typesafe.config._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import telemetry.SelfFirstClassLoader

class AvroParquetWriterLike(parquetFile: Path, schema: Schema, compression: String, blockSize: Int, pageSize: Int, enableDict: Boolean, hadoopConf: Configuration) {
  private val classLoader = SelfFirstClassLoader(AvroParquetWriterLike.path)

  private val codec = classLoader.loadClass("org.apache.parquet.hadoop.metadata.CompressionCodecName")
  private val snappy  = codec.getEnumConstants().find(_.toString.equals(compression)).get

  private var cls = classLoader.loadClass("org.apache.parquet.avro.AvroParquetWriter")
  private val ctor = cls.getConstructor(classOf[Path], classOf[Schema], codec, classOf[Int], classOf[Int], classOf[Boolean], classOf[Configuration])
  private val parquetWriter = ctor.newInstance(parquetFile, schema, snappy, Int.box(blockSize), Int.box(pageSize), Boolean.box(enableDict), hadoopConf)

  def close() {
    cls.getMethod("close").invoke(parquetWriter)
  }

  def getDataSize(): Long = {
    cls.getMethod("getDataSize").invoke(parquetWriter).asInstanceOf[Long]
  }

  def write(record: GenericRecord) {
    cls.getMethod("write", classOf[AnyRef]).invoke(parquetWriter, record)
  }
}

object AvroParquetWriterLike{
  private val conf = ConfigFactory.load()
  private val path = conf.getString("app.jarDirectory")

  private val hadoopConf = new Configuration()
  private val blockSize = 128 * 1024 * 1024
  private val pageSize = 1 * 1024 * 1024
  private val compression = "SNAPPY"
  private val enableDict = true

  hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  def apply(parquetFile: Path, schema: Schema): AvroParquetWriterLike = {
    new AvroParquetWriterLike(parquetFile, schema, compression, blockSize, pageSize, enableDict, hadoopConf)
  }
}
