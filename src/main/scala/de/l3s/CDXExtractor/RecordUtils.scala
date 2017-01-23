package de.l3s.CDXExtractor

import de.l3s.CDXExtractor.data.FileOffset
import java.io.InputStream
import org.apache.hadoop.io.Text
import org.archive.io.{ArchiveReaderFactory, ArchiveRecord}
import cats._
import cats.data._
import cats.implicits._

object RecordUtils {

  def textToNameAndOffset(text: Text): Either[String, FileOffset] = {
    val separator = ":"
    val split = text.toString.split(separator)

    if (split.size == 2)
      Either
        .catchNonFatal(split(1).toLong)
        .fold(
          error =>
            Left(
              "Could not convert ${split(1)} to type long for input: ${text.toString}"),
          l => Right(FileOffset(split(0), l)))
    else
      Left(s"Too few or two many colons in input: ${text.toString}")
  }

  def streamToArchiveRecord(
      fileOffset: FileOffset,
      stream: InputStream): Either[String, ArchiveRecord] = {
    Either.catchNonFatal {
      val atBeginning = if (fileOffset.offset == 0) true else false
      val archiveReader =
        ArchiveReaderFactory.get(fileOffset.fileName, stream, atBeginning)
      val record = archiveReader.get()
      archiveReader.close()
      record
    }.leftMap(t => t.getMessage)
  }
}
