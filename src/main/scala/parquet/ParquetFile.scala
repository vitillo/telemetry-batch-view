package telemetry.parquet

import com.typesafe.config._
import java.net.URI
import java.rmi.dgc.VMID
import java.util.logging.Logger
import org.apache.avro
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scala.collection.JavaConverters._

object ParquetFile {
  private val conf = ConfigFactory.load()
  private val parquetLogger = Logger.getLogger("org.apache.parquet")
  private val hadoopConf = new Configuration()

  hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  private def temporaryFileName(): Path = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    val vmid = new VMID().toString().replaceAll(":|-", "")
    val uri = URI.create(s"file:///$tmpDir/$vmid.tmp")
    return new Path(uri)
  }

  def serialize(data: Iterator[GenericRecord], schema: Schema): Path = {
    val blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE
    val pageSize = ParquetWriter.DEFAULT_PAGE_SIZE
    val enableDict = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED
    val parquetFile = temporaryFileName()
    val parquetWriter = new AvroParquetWriter[GenericRecord](parquetFile, schema, CompressionCodecName.SNAPPY, blockSize, pageSize, enableDict, hadoopConf)

    // Disable Parquet logging
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)

    for (d <- data) {
      parquetWriter.write(d)
    }

    parquetWriter.close
    parquetFile
  }

  def deserialize(filename: String): Seq[GenericRecord] = {
    val path = new Path(filename)
    val reader = new AvroParquetReader[GenericRecord](path)

    def loop(l: List[GenericRecord]): List[GenericRecord] = Option(reader.read) match {
      case Some(record) => record :: loop(l)
      case None => l
    }

    loop(List[GenericRecord]())
  }
}
