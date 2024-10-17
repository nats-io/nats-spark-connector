package org.apache.spark.sql.nats

import munit.{FunSuite }
import java.util.zip.Deflater

class NatMessageToSparkRowTest extends FunSuite  {

  // useful for compressing strings for test
  // à la GPT
  def compress(data: String): Array[Byte] = {
    val bytes = data.getBytes("UTF-8") // Convert string to bytes
    val deflater = new Deflater()
    deflater.setInput(bytes)
    deflater.finish()

    val compressedData = new Array[Byte](bytes.length * 10) // Compressed data can potentially be larger
    val compressedDataSize = deflater.deflate(compressedData)

    compressedData.take(compressedDataSize) // Return only the used portion of the buffer
  }

  test("it correctly decompresses simple string") {
    val originalString = "Hello there"
    val compressedBytes = compress(originalString)
    val decompressBytes = MessageToSparkRow.decompress(compressedBytes)
    val decompressedString = new String(decompressBytes, "UTF-8")

    assertEquals( 
      originalString,
      decompressedString
    )
  }

  test("it correctly decompresses highly compressable string") {
    val originalString = "Wheels on the bus go" + (" round and round" * 100000)
    val compressedBytes = compress(originalString)
    val decompressBytes = MessageToSparkRow.decompress(compressedBytes)
    val decompressedString = new String(decompressBytes, "UTF-8")

    // prints: 1600020,3156
    // println(decompressBytes.length, compressedBytes.length)

    assertEquals( 
      originalString,
      decompressedString
    )
  }
}