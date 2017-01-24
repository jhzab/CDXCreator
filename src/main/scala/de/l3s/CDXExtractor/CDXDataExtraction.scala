package de.l3s.CDXExtractor

import java.io.{ByteArrayInputStream, EOFException, IOException, InputStream}
import java.util.zip.GZIPInputStream

import de.l3s.CDXExtractor.RecordUtils.{streamToArchiveRecord, textToNameAndOffset}
import de.l3s.CDXExtractor.data.{CDXRecord, FileOffset}
import de.l3s.concatgz.io.FileBackedBytesWritable
import java.security.{DigestInputStream, MessageDigest}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.archive.format.http.{HttpHeaders, HttpResponse, HttpResponseParser}
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
    /* TODO: use a counting InputStream to read data only once,
     * OTOH this would also require to change the CDX object later on. */
    val data = IOUtils.toByteArray(is)
    val gzipSize = data.length

    for {
      /* Worst case is that a 1GB WARC file with ONE page gets copied into memory */
      gzip <- Either
        .catchNonFatal(new GZIPInputStream(new ByteArrayInputStream(data)))
        .leftMap { e =>
          e match {
            /* TODO: find out why some inputs result in a EOFException some others don't (even on EOF) */
            case e: EOFException =>
              //e.printStackTrace()
              "Encountered end of file."
            case e: Exception =>
              //e.printStackTrace()
              e.getMessage
            case _ => "Could not match exception"
          }
        }
      fileOffset <- textToNameAndOffset(text)
      archiveRecord <- streamToArchiveRecord(fileOffset, gzip)
      cdx <- getCDXFromWARC(archiveRecord, fileOffset, gzipSize)
    } yield cdx
  }

  private[this] def getHttpReponse(is: InputStream): Either[String, HttpResponse] = {
    Either.catchNonFatal {
      val parser = new HttpResponseParser()
      parser.parse(is)
    }.leftMap(e => s"Could not extract http reponse: ${e.getMessage}")
  }

  private[this] def getHttpStatus(hR: HttpResponse): Either[String, Int] = {
    Either.catchNonFatal {
        hR.getMessage.getStatus
    }.leftMap(e => s"Could not extract http status code: ${e.getMessage}")
  }

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

  private[this] def getChecksum(is: InputStream): Either[String, String] = {
    val digIS = new DigestInputStream(is, MessageDigest.getInstance("sha1"))

    Either.catchNonFatal {
      while (digIS.read() != -1) ()
      val digest = digIS.getMessageDigest.digest()
      Base32.encode(digest)
    }.leftMap(e => e.getMessage)
  }

  private[this] def getRedirect(httpHeaders: HttpHeaders): Either[String, String] = {
    Either.catchNonFatal {
      val locationHeader = httpHeaders.get("Location")
      if (locationHeader != null) {
        locationHeader.getValue
      } else
        "-"
    }.leftMap(e => s"Error accessing Location header: ${e.getMessage}")
  }

  def getCDXFromWARC(aR: ArchiveRecord,
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

    for {
      date <- getDate(archiveHeader)
      mimeType <- getMimeType(archiveHeader)
      httpResponse <- getHttpReponse(aR)
      responseCode <- getHttpStatus(httpResponse)
      redirect <- getRedirect(httpResponse.getHeaders)
      checksum <- getChecksum(aR)
    } yield
      CDXRecord(canonizedUrl,
                date,
                url,
                mimeType,
                responseCode.toString,
                checksum,
                redirect,
                metaTags,
                compressedSize.toString,
                offset,
                fileName)
  }
}
