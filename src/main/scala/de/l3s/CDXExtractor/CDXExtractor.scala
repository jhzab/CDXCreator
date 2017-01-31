package de.l3s.CDXExtractor

import org.apache.spark.sql.SparkSession
import de.l3s.CDXExtractor.CDXDataExtraction.getCDXFromTextAndStream
import de.l3s.CDXExtractor.data.CDXExtractorArguments
import de.l3s.concatgz.io.{ConcatGzipInputFormat, FileBackedBytesWritable}
import org.apache.hadoop.io.Text

object CDXExtractor {
  def parseArguments(args: Array[String]): Option[CDXExtractorArguments] = {
    val parser =
      new scopt.OptionParser[CDXExtractorArguments]("CDXExtractor") {
        head("CDXExtractor", "0.1")
        opt[String]("input")
          .required()
          .valueName("<file>")
          .action((x, config) => config.copy(input = x))
        opt[String]("output")
          .required()
          .valueName("<file>")
          .action((x, config) => config.copy(output = x))
      }
    parser.parse(args, CDXExtractorArguments())
  }

  def createCDXData(config: CDXExtractorArguments): Unit = {
    val session = SparkSession.builder().appName("CDXCreator").getOrCreate()
    import session.implicits._

    val warcRecords = session.sparkContext
      .newAPIHadoopFile(config.input,
                        classOf[ConcatGzipInputFormat],
                        classOf[Text],
                        classOf[FileBackedBytesWritable])
      .map {
        case (text, stream) =>
          getCDXFromTextAndStream(text, stream)
      }
      .coalesce(7000)

    /*
    val errors = warcRecords.collect{ case Left(error) => error}.toDS
    val errorCount = errors.count

    System.err.println(s"\n$errorCount error(s) while creating CDX files\n")
    if (errorCount > 0) {
      System.err.println(s"First few errors: ")
      errors.head(20).foreach(e => System.err.println("Error: " + e))
    }
     */

    val validWarcRecords = warcRecords.collect { case Right(cdx) => cdx }.toDS

    validWarcRecords.write.option("sep", " ").csv(config.output)
  }

  def main(args: Array[String]): Unit = {
    parseArguments(args) match {
      case Some(arguments) =>
        createCDXData(arguments)
      case None =>
    }
  }
}
