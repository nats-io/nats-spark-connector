package natsconnector.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.streaming.Offset
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

class NatsOffsetSpec extends AnyFlatSpec with Matchers {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  // What Spark hands back after a restart: an opaque Offset carrying the JSON read from the
  // checkpoint's offset log (Spark's own SerializedOffset, whose package moved in Spark 4.1).
  private def restoredOffset(serialized: String): Offset = new Offset {
    override def json: String = serialized
  }

  "NatsOffset" should "serialize and deserialize correctly" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2", "batch3"))
    val offset = NatsOffset(Some(batchInfo))
    
    val json = offset.json
    json should include("\"batchIdList\":[\"batch1\",\"batch2\",\"batch3\"]")
  }

  it should "handle None offset correctly" in {
    val offset = NatsOffset(None)
    val json = offset.json
    // The JSON for None offset should be a simple object
    json should (include("null") or equal("{}"))
    
    // Test that it represents the None state correctly
    offset.offset should be(None)
    
    // Test basic equality
    val offset2 = NatsOffset(None)
    offset should equal(offset2)
  }

  it should "implement equals correctly for NatsOffset instances" in {
    val batchInfo1 = NatsBatchInfo(List("batch1", "batch2"))
    val batchInfo2 = NatsBatchInfo(List("batch1", "batch2"))
    val batchInfo3 = NatsBatchInfo(List("batch3", "batch4"))
    
    val offset1 = NatsOffset(Some(batchInfo1))
    val offset2 = NatsOffset(Some(batchInfo2))
    val offset3 = NatsOffset(Some(batchInfo3))
    val offset4 = NatsOffset(None)
    val offset5 = NatsOffset(None)
    
    offset1 should equal(offset2)
    offset1 should not equal offset3
    offset4 should equal(offset5)
    offset1 should not equal offset4
  }

  it should "handle string and object comparisons correctly" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val offset = NatsOffset(Some(batchInfo))
    
    // Invalid JSON should definitely return false
    offset.equals("invalid json") should be(false)
    
    // Test with a simple string that's not JSON
    offset.equals("not json at all") should be(false)
    
    // Test with null
    offset.equals(null) should be(false)
    
    // Test with non-string, non-offset object
    offset.equals(42) should be(false)
    offset.equals(List(1, 2, 3)) should be(false)

    // Its own JSON form compares equal
    offset.equals(offset.json) should be(true)
  }

  it should "convert from a restored (serialized) offset correctly" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val originalOffset = NatsOffset(Some(batchInfo))
    val serializedOffset = restoredOffset(originalOffset.json)
    
    val convertedOffset = NatsOffset.fromJson(serializedOffset.json)
    convertedOffset should be(Some(originalOffset))
  }

  it should "handle convert method with NatsOffset input" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val offset = NatsOffset(Some(batchInfo))
    
    val result = NatsOffset.convert(offset)
    result should be(Some(offset))
  }

  it should "handle convert method with a restored (serialized) offset input" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val originalOffset = NatsOffset(Some(batchInfo))
    val serializedOffset = restoredOffset(originalOffset.json)
    
    val result = NatsOffset.convert(serializedOffset)
    result should be(defined)
    result.get should equal(originalOffset)
  }

  it should "restore the None offset from its serialized forms" in {
    NatsOffset.convert(restoredOffset(NatsOffset(None).json)) should be(Some(NatsOffset(None)))
    NatsOffset.convert(restoredOffset("{}")) should be(Some(NatsOffset(None)))
    NatsOffset.convert(restoredOffset("{\"offset\":null}")) should be(Some(NatsOffset(None)))
  }

  it should "return None for unsupported offset types" in {
    val unsupportedOffset = new org.apache.spark.sql.execution.streaming.Offset {
      override def json: String = "{\"unsupported\": true}"
    }
    
    val result = NatsOffset.convert(unsupportedOffset)
    result should be(None)
  }

  it should "return None for offsets that are not JSON objects" in {
    NatsOffset.convert(restoredOffset("42")) should be(None)
    NatsOffset.convert(restoredOffset("not json")) should be(None)
    NatsOffset.convert(restoredOffset("[1,2,3]")) should be(None)
  }

  it should "handle empty batch list" in {
    val batchInfo = NatsBatchInfo(List.empty)
    val offset = NatsOffset(Some(batchInfo))
    
    val json = offset.json
    json should include("\"batchIdList\":[]")
    
    val serializedOffset = restoredOffset(json)
    val convertedOffset = NatsOffset.convert(serializedOffset)
    convertedOffset should be(Some(offset))
  }

  it should "handle large batch lists" in {
    val largeBatchList = (1 to 1000).map(i => s"batch$i").toList
    val batchInfo = NatsBatchInfo(largeBatchList)
    val offset = NatsOffset(Some(batchInfo))
    
    val json = offset.json
    json should include("batch1")
    json should include("batch1000")
    
    val serializedOffset = restoredOffset(json)
    val convertedOffset = NatsOffset.convert(serializedOffset)
    convertedOffset should be(Some(offset))
  }
}
