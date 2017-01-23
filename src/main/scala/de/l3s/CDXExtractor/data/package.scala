package de.l3s.CDXExtractor

import de.l3s.concatgz.io.FileBackedBytesWritable

package object data {
  final case class CDXExtractorArguments(input: String = "",
                                         output: String = "")

  /** CDX definition.
    *  This follows https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
    *  with the CDX definition: CDX N b a m s k r M S V g
    *  We use the above definition since our existing CDX data is also in that format.
    */
  final case class CDXRecord(canonizedURL: String,
                             date: String,
                             originalURL: String,
                             mime: String,
                             responseCode: String,
                             newStyleChecksum: String,
                             redirect: String,
                             metaTags: String,
                             compressedSize: String,
                             offset: Long,
                             fileName: String)

  final case class FileOffset(fileName: String, offset: Long)

  final case class CompressedRecord(fileName: String,
                                    offset: Long,
                                    byteStream: FileBackedBytesWritable)

}
