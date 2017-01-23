package de.l3s.CDXExtractor

import java.io.{ByteArrayInputStream, EOFException, IOException, InputStream}
import java.util.zip.GZIPInputStream

import de.l3s.CDXExtractor.RecordUtils.{streamToArchiveRecord, textToNameAndOffset}
import de.l3s.CDXExtractor.data.{CDXRecord, FileOffset}
import de.l3s.concatgz.io.FileBackedBytesWritable
import java.security.{DigestInputStream, MessageDigest}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.archive.format.http.HttpResponseParser
import org.archive.io.{ArchiveRecord, ArchiveRecordHeader}
import org.archive.util.{Base32, SURT}
import cats._
import cats.data._
import cats.implicits._

object CDXDataExtraction {
  def ioStreamToDigestStream(is: InputStream): Either[String, DigestInputStream] = {
    Either.catchNonFatal(new DigestInputStream(is,
      MessageDigest.getInstance("sha1"))).leftMap{
      /* Ignore IO errors while reading the stream */
      case e: java.io.IOException => ""
      case t: Throwable => t.getMessage
    }
  }

  def getCDXFromTextAndStream(
      text: Text,
      stream: FileBackedBytesWritable): Either[String, CDXRecord] = {
    val is = stream.getBytes.openBufferedStream()
    /* Copy the GZIP stream into memory to check it's size */
    /* TODO: use a counting InputStream to read data only once */
    val data = IOUtils.toByteArray(is)
    val gzipSize = data.length
    // println(s"${text.toString} - ${is.available} - $gzipSize")

    for {
      /* Worst case is that a 1GB WARC file with ONE page gets copied into memory */
      gzip <- Either
        .catchNonFatal(new GZIPInputStream(new ByteArrayInputStream(data)))
        .leftMap { e =>
          e match {
            /* TODO: find out why some inputs result in a EOFException some others don't (even on EOF) */
            case e: EOFException =>
              e.printStackTrace
              "Encountered end of file."
            case e: Exception =>
              e.printStackTrace
              e.getMessage
            case _ => "Could not match exception"
          }
        }
      digIS <- ioStreamToDigestStream(gzip)
      fileOffset <- textToNameAndOffset(text)
      archiveRecord <- streamToArchiveRecord(fileOffset, digIS)
      cdx <- getCDXFromWARC(archiveRecord, digIS, fileOffset, gzipSize)
    } yield cdx
  }

  private[this] def getHeaderValue(httpHeaders: java.util.Map[String, String],
                                   key: String): Option[String] = {
    val value: String = httpHeaders.get(key)
    if (value != null)
      Option(value)
    else None
  }

  private[this] def getHttpStatus(is: InputStream): Either[String, String] = {
    Either.catchNonFatal {
      val parser = new HttpResponseParser()
      val httpResponse = parser.parse(is)
      httpResponse.getMessage.getStatus.toString
    }.leftMap(e => s"Could not extract http status code: ${e.getMessage}")
  }
  /*
  private[this] def getFromRecordHeader[T](recordHeader: ArchiveRecordHeader)(f: ArchiveRecordHeader => T): Either[String, T] = {
    Either
      .catchNonFatal(f(recordHeader))
  }
   */
  private[this] def getMimeType(
      recordHeader: ArchiveRecordHeader): Either[String, String] =
    Either
      .catchNonFatal(recordHeader.getMimetype)
      .leftMap(e => s"Could not extract mime type: ${e.getMessage}")

  /* FIXME: needs to be normalized */
  private[this] def getDate(
      recordHeader: ArchiveRecordHeader): Either[String, String] =
    Either
      .catchNonFatal(recordHeader.getDate)
      .leftMap(e => s"Could not extract date: ${e.getMessage}")

  private[this] def getChecksum(aR: ArchiveRecord, is: DigestInputStream): Either[String, String] = {
    Either.catchNonFatal {
      try {
        while (aR.read() != -1) ()
      } catch {
        case e: IOException => /* Ignore Stream is closed IOException */
      }
      val digest = is.getMessageDigest.digest()
      Base32.encode(digest)
    }.leftMap(e => e.getMessage + "1")
  }

  def getCDXFromWARC(aR: ArchiveRecord,
                     digIS: DigestInputStream,
                     fO: FileOffset,
                     compressedSize: Int): Either[String, CDXRecord] = {
    /* can't fail */
    val offset = fO.offset
    val fileName = fO.fileName
    val archiveHeader = aR.getHeader
    val url = archiveHeader.getUrl
    val metaTags = "-"

    /* might fail */
    val canonizedUrl = SURT.fromURI(url).replace("http://(", "")
    val redirect = "-"
    val digLocal = new DigestInputStream(aR, MessageDigest.getInstance("sha1"))

    for {
      date <- getDate(archiveHeader)
      mimeType <- getMimeType(archiveHeader)
      responseCode <- getHttpStatus(digLocal)
      checksum <- getChecksum(aR, digLocal)
    } yield
      CDXRecord(canonizedUrl,
                date,
                url,
                mimeType,
                responseCode,
                checksum,
                redirect,
                metaTags,
                compressedSize.toString,
                offset,
                fileName)
  }
}
