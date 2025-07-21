package validation

import natsconnector.{NatsConfig, NatsConfigSource, NatsConfigSink}
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

/**
 * Test to verify configuration isolation without requiring NATS server connection.
 * This tests that multiple jobs can have separate configurations that don't interfere.
 */
object ConfigIsolationTest extends App {
  println("Starting configuration isolation test...")
  
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
          testConfigIsolation(threadId)
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
    println(s"Test completed. Successful threads: ${successCount.get()}, Failed threads: ${errorCount.get()}")
    
    if (successCount.get() == numThreads && errorCount.get() == 0) {
      println("✓ All threads completed successfully - configuration isolation verified!")
      
      // Additional verification: check that we have separate config instances
      val config1 = NatsConfigSource.getConfig("test1")
      val config2 = NatsConfigSource.getConfig("test2")
      
      if (config1 != config2) {
        println("✓ Different config keys produce different instances")
      } else {
        println("✗ Same instance returned for different config keys")
        System.exit(1)
      }
      
      val config1Again = NatsConfigSource.getConfig("test1")
      if (config1 == config1Again) {
        println("✓ Same config key produces same instance")
      } else {
        println("✗ Different instance returned for same config key")
        System.exit(1)
      }
      
      System.exit(0)
    } else {
      println("✗ Some threads failed - configuration isolation issues detected")
      System.exit(1)
    }
  } else {
    println("✗ Configuration isolation test timed out")
    System.exit(1)
  }
  
  executor.shutdown()
  
  def testConfigIsolation(threadId: Int): Unit = {
    val random = new Random(threadId)
    
    for (i <- 1 to operationsPerThread) {
      val configKey = s"thread-$threadId-config-$i"
      val streamName = s"TestStream_${threadId}_$i"
      val subjects = s"subject${threadId}.$i"
      
      try {
        // Test source config - just configuration, no connection
        val sourceConfig = NatsConfigSource.getConfig(configKey)
        
        // Set basic parameters without connecting
        sourceConfig.host = "localhost"
        sourceConfig.port = "4222"
        sourceConfig.streamName = Some(streamName)
        sourceConfig.streamSubjects = Some(subjects)
        sourceConfig.durable = Some(s"durable_${threadId}_$i")
        
        // Verify configuration was set correctly
        assert(sourceConfig.streamName.contains(streamName), 
          s"Stream name mismatch for thread $threadId operation $i")
        assert(sourceConfig.streamSubjects.contains(subjects),
          s"Subjects mismatch for thread $threadId operation $i")
        
        // Test sink config
        val sinkConfigKey = s"sink-thread-$threadId-config-$i"
        val sinkConfig = NatsConfigSink.getConfig(sinkConfigKey)
        
        sinkConfig.host = "localhost"
        sinkConfig.port = "4222"
        
        // Verify sink configuration
        assert(sinkConfig.host == "localhost",
          s"Host mismatch for sink thread $threadId operation $i")
        assert(sinkConfig.port == "4222",
          s"Port mismatch for sink thread $threadId operation $i")
        
        // Test config key uniqueness - create another config with different settings
        val anotherThreadConfig = NatsConfigSource.getConfig(s"thread-${threadId + 100}-config-$i")
        anotherThreadConfig.host = "different-host"
        anotherThreadConfig.port = "4223"
        anotherThreadConfig.streamName = Some("DifferentStream")
        anotherThreadConfig.streamSubjects = Some("different.subject")
        anotherThreadConfig.durable = Some("different_durable")
        
        // Verify that configurations don't interfere with each other
        assert(sourceConfig.host == "localhost" && anotherThreadConfig.host == "different-host",
          s"Configuration interference detected in thread $threadId operation $i")
        
        assert(sourceConfig.streamName.get == streamName && 
               anotherThreadConfig.streamName.get == "DifferentStream",
          s"Stream name interference detected in thread $threadId operation $i")
        
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