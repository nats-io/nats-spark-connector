package natsconnector.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

class NatsOffsetSpec extends AnyFlatSpec with Matchers {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

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
  }

  it should "convert from SerializedOffset correctly" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val originalOffset = NatsOffset(Some(batchInfo))
    val serializedOffset = SerializedOffset(originalOffset.json)
    
    val convertedOffset = NatsOffset(serializedOffset)
    convertedOffset should equal(originalOffset)
  }

  it should "handle convert method with NatsOffset input" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val offset = NatsOffset(Some(batchInfo))
    
    val result = NatsOffset.convert(offset)
    result should be(Some(offset))
  }

  it should "handle convert method with SerializedOffset input" in {
    val batchInfo = NatsBatchInfo(List("batch1", "batch2"))
    val originalOffset = NatsOffset(Some(batchInfo))
    val serializedOffset = SerializedOffset(originalOffset.json)
    
    val result = NatsOffset.convert(serializedOffset)
    result should be(defined)
    result.get should equal(originalOffset)
  }

  it should "return None for unsupported offset types" in {
    val unsupportedOffset = new org.apache.spark.sql.execution.streaming.Offset {
      override def json: String = "{\"unsupported\": true}"
    }
    
    val result = NatsOffset.convert(unsupportedOffset)
    result should be(None)
  }

  it should "handle empty batch list" in {
    val batchInfo = NatsBatchInfo(List.empty)
    val offset = NatsOffset(Some(batchInfo))
    
    val json = offset.json
    json should include("\"batchIdList\":[]")
    
    val serializedOffset = SerializedOffset(json)
    val convertedOffset = NatsOffset(serializedOffset)
    convertedOffset should equal(offset)
  }

  it should "handle large batch lists" in {
    val largeBatchList = (1 to 1000).map(i => s"batch$i").toList
    val batchInfo = NatsBatchInfo(largeBatchList)
    val offset = NatsOffset(Some(batchInfo))
    
    val json = offset.json
    json should include("batch1")
    json should include("batch1000")
    
    val serializedOffset = SerializedOffset(json)
    val convertedOffset = NatsOffset(serializedOffset)
    convertedOffset should equal(offset)
  }
}