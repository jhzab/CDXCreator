package de.l3s.CDXExtractor

import java.io.InputStream
import java.util.zip.GZIPInputStream

import de.l3s.CDXExtractor.RecordUtils.{
  streamToArchiveRecord,
  textToNameAndOffset
}
import de.l3s.CDXExtractor.data.{CDXRecord, FileOffset}
import de.l3s.concatgz.io.FileBackedBytesWritable
import java.security.{DigestInputStream, MessageDigest}

import org.apache.hadoop.io.Text
import org.archive.format.http.{HttpHeaders, HttpResponse, HttpResponseParser}
import org.archive.io.{ArchiveRecord, ArchiveRecordHeader}
import org.archive.util.{Base32, SURT}
import cats.implicits._
import com.google.common.io.CountingInputStream

object CDXDataExtraction {
  def getCDXFromTextAndStream(
      text: Text,
      stream: FileBackedBytesWritable): Either[String, CDXRecord] = {
    val is = stream.getBytes.openBufferedStream()
    val countingStream = new CountingInputStream(is)
    for {
      gzip <- Either
        .catchNonFatal(new GZIPInputStream(countingStream))
        .leftMap(t => t.getMessage)
      fileOffset <- textToNameAndOffset(text)
      archiveRecord <- streamToArchiveRecord(fileOffset, gzip)
      cdx <- getCDXFromWARC(archiveRecord, fileOffset)
    } yield cdx.copy(compressedSize = countingStream.getCount.toString)
  }

  private[this] def getHttpReponse(
      is: InputStream): Either[String, HttpResponse] = {
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

  private[this] def getRedirect(
      httpHeaders: HttpHeaders): Either[String, String] = {
    Either.catchNonFatal {
      val locationHeader = httpHeaders.get("Location")
      if (locationHeader != null) {
        locationHeader.getValue
      } else
        "-"
    }.leftMap(e => s"Error accessing Location header: ${e.getMessage}")
  }

  private[this] def getArchiveHeader(
      aR: ArchiveRecord): Either[String, ArchiveRecordHeader] = {
    val header = aR.getHeader

    if (header == null)
      Left("Could not extract header for record")
    else
      Right(header)
  }

  private[this] def getUrl(
      header: ArchiveRecordHeader): Either[String, String] = {
    val url = header.getUrl

    if (url == null)
      Left("Could not extract url from archive record header")
    else
      Right(url)
  }

  def getCDXFromWARC(aR: ArchiveRecord,
                     fO: FileOffset): Either[String, CDXRecord] = {
    /* can't fail */
    val offset = fO.offset
    val fileName = fO.fileName
    val metaTags = "-"

    for {
      archiveHeader <- getArchiveHeader(aR)
      url <- getUrl(archiveHeader)
      date <- getDate(archiveHeader)
      mimeType <- getMimeType(archiveHeader)
      httpResponse <- getHttpReponse(aR)
      responseCode <- getHttpStatus(httpResponse)
      redirect <- getRedirect(httpResponse.getHeaders)
      checksum <- getChecksum(aR)
    } yield
      CDXRecord(SURT.fromURI(url).replace("http://(", ""),
                date,
                url,
                mimeType,
                responseCode.toString,
                checksum,
                redirect,
                metaTags,
                null,
                offset,
                fileName)
  }
}
