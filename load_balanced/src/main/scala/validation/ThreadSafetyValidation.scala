package validation

import natsconnector.{NatsConfig, NatsConfigSource, NatsConfigSink}
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

/**
 * Validation script to verify thread safety of the configuration management system.
 */
object ThreadSafetyValidation extends App {
  println("Starting thread safety validation...")
  
  val numThreads = 5
  val operationsPerThread = 50
  val testTimeout = 30 // seconds
  
  val executor: ExecutorService = Executors.newFixedThreadPool(numThreads)
  val latch = new CountDownLatch(numThreads)
  val successCount = new AtomicInteger(0)
  val errorCount = new AtomicInteger(0)
  
  println(s"Testing with $numThreads threads, $operationsPerThread operations each")
  
  // Test concurrent access to configuration objects
  for (threadId <- 1 to numThreads) {
    executor.submit(new Runnable {
      override def run(): Unit = {
        try {
          testConcurrentConfigAccess(threadId)
          successCount.incrementAndGet()
          println(s"Thread $threadId completed successfully")
        } catch {
          case e: Exception =>
            println(s"Thread $threadId failed: ${e.getMessage}")
            e.printStackTrace()
            errorCount.incrementAndGet()
        } finally {
          latch.countDown()
        }
      }
    })
  }
  
  // Wait for all threads to complete
  if (latch.await(testTimeout, TimeUnit.SECONDS)) {
    println(s"Validation completed. Successful threads: ${successCount.get()}, Failed threads: ${errorCount.get()}")
    
    if (successCount.get() == numThreads && errorCount.get() == 0) {
      println("✓ All threads completed successfully - thread safety verified!")
      System.exit(0)
    } else {
      println("✗ Some threads failed - thread safety issues detected")
      System.exit(1)
    }
  } else {
    println("✗ Thread safety validation timed out")
    System.exit(1)
  }
  
  executor.shutdown()
  
  def testConcurrentConfigAccess(threadId: Int): Unit = {
    val random = new Random(threadId)
    
    for (i <- 1 to operationsPerThread) {
      val configKey = s"thread-$threadId-config-$i"
      val streamName = s"TestStream_${threadId}_$i"
      val subjects = s"subject${threadId}.$i"
      
      try {
        // Test source config
        val sourceConfig = NatsConfigSource.getConfig(configKey)
        
        val sourceParams = Map(
          "nats.host" -> "localhost",
          "nats.port" -> "4222",
          "nats.stream.name" -> streamName,
          "nats.stream.subjects" -> subjects,
          "nats.msg.ack.wait.secs" -> "60",
          "nats.durable.name" -> s"durable_${threadId}_$i"
        )
        
        sourceConfig.setConnection(sourceParams)
        
        // Verify configuration was set correctly
        assert(sourceConfig.streamName.contains(streamName), 
          s"Stream name mismatch for thread $threadId operation $i")
        assert(sourceConfig.streamSubjects.contains(subjects),
          s"Subjects mismatch for thread $threadId operation $i")
        
        // Test sink config
        val sinkConfigKey = s"sink-thread-$threadId-config-$i"
        val sinkConfig = NatsConfigSink.getConfig(sinkConfigKey)
        
        val sinkParams = Map(
          "nats.host" -> "localhost", 
          "nats.port" -> "4222"
        )
        
        sinkConfig.setConnection(sinkParams)
        
        // Verify sink configuration
        assert(sinkConfig.host == "localhost",
          s"Host mismatch for sink thread $threadId operation $i")
        assert(sinkConfig.port == "4222",
          s"Port mismatch for sink thread $threadId operation $i")
        
        // Test config key uniqueness
        val anotherThreadConfig = NatsConfigSource.getConfig(s"thread-${threadId + 100}-config-$i")
        anotherThreadConfig.setConnection(Map(
          "nats.host" -> "different-host",
          "nats.port" -> "4223",
          "nats.stream.name" -> "DifferentStream",
          "nats.stream.subjects" -> "different.subject",
          "nats.msg.ack.wait.secs" -> "30",
          "nats.durable.name" -> "different_durable"
        ))
        
        // Verify that configurations don't interfere with each other
        assert(sourceConfig.host == "localhost" && anotherThreadConfig.host == "different-host",
          s"Configuration interference detected in thread $threadId operation $i")
        
        // Small random delay to increase chance of race conditions
        if (random.nextInt(10) == 0) {
          Thread.sleep(random.nextInt(5))
        }
        
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Thread $threadId operation $i failed", e)
      }
    }
  }
}