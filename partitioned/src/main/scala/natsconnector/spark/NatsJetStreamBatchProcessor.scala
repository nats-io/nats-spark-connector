package natsconnector.spark

import natsconnector.NatsConfigSource

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.io.IOException

import util.control.Breaks._

import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Map => MutableMap}

//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
import org.apache.log4j.Logger

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import io.nats.client.Message
import io.nats.client.KeyValue
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import io.nats.client.JetStreamSubscription
import natsconnector.NatsLogger

// How batching works:
// There is one thread created for each Nats JetStream partition, which should be setup with the same number
// as the number of Spark workers.
// When Spark is ready to get a batch, the function processNatsBatching releases the threads, which are waiting on
// the countdown latch, by zeroing the latch. Each thread then gets a newly created latch, proceeds to 
// obtain a Nats batch and goes back to waiting for the latch to be zeroed again during a new cycle of batch acquisition.
//
// The conceptual algorithm for each thread is:
//   1. 
// 

final object NatsJetStreamBatchProcessor {
    //val logger:Logger = LoggerFactory.getLogger(classOf[NatsJetStreamStreamingSource])
    //private val partitionThreads: ListBuffer[Thread] = ListBuffer.empty[Thread]
    private val partitionThreadMap: MutableMap[Thread, Int] = MutableMap()
    private val sparkContext: SparkContext = SparkSession.getDefaultSession.get.sparkContext
    val logger:Logger = NatsLogger.logger
    private val numWorkers = this.sparkContext.getConf.get("spark.executor.instances").toInt
    private var latch = new CountDownLatch(this.numWorkers+1)
    val buffer: ListBuffer[Message] = ListBuffer.empty[Message]
    private var startOffsetMap:MutableMap[Int, Long] = MutableMap()
    private var endOffsetMap:MutableMap[Int, Long] = MutableMap()


    def processNatsBatching(
            startJSOffsetMap:MutableMap[Int, Long], 
            endJSOffsetMap:MutableMap[Int, Long]
            ): ListBuffer[Message] = {
    
        // reload latest offset maps so threads can use them
        this.startOffsetMap.clear()
        this.startOffsetMap ++= startJSOffsetMap

        this.endOffsetMap.clear()
        this.endOffsetMap ++= endJSOffsetMap


        // make sure to release the latch so threads may continue. Some threads may have died w/out performing a countdown.
        while(this.latch.getCount() > 0) {
            this.latch.countDown()
        }
         
        // Restart threads if necessary
        testPartitionThreadsSanity()

        // Wait for threads to be done loading messages into the shared buffer
        while(this.latch.getCount() > 1) {
            Thread.sleep(1L)
        }

        // copy thread-filled shared buffer into returned buffer and zero local buffer for next round.
        val newBuffer = new ListBuffer[Message]()
        newBuffer.insertAll(0, this.buffer)
        this.buffer.clear()
        return  newBuffer
    }

     def startBatchingThreads(numThreadsToStart: Int): Unit = {
        (0 until numThreadsToStart).map(nr => new Thread(new PartitionThread() {
              override def run(): Unit = {
                val thread = Thread.currentThread()
                // create thread affinity and register thread for management
                val partition = registerThread(thread, NatsConfigSource.config.kvm)
                // TODO: register with offset
                while(true) {
                    doLatch()
                    val offsets = getStartAndEndOffsets(partition)
                    val startJSOffset = offsets._1
                    // if the stream was cleared then the spark-stored end-offset may be
                    // greater than the actual nats end-offset.
                    val jsm = NatsConfigSource.config.jsm
                    val jsi = jsm.getStreamInfo(NatsConfigSource.config.streamPrefix + "-" + partition.toString())
                    val last = jsi.getStreamState().getLastSequence()
                    val endJSOffset = if(offsets._2 > last) last else offsets._2

                    this.logger.debug(s"partition $partition: start:$startJSOffset end:$endJSOffset")
println(s"original==> partition $partition: start:${offsets._1} end:${offsets._2}")
println(s"adjusted==> partition $partition: start:$startJSOffset end:$endJSOffset")
                    val opso = PushSubscribeOptions
                      .builder()
                      .stream(NatsConfigSource.config.streamPrefix + "-" + partition)
                      .ordered(true)
                      .configuration(
                        ConsumerConfiguration.builder()
                            .deliverPolicy(DeliverPolicy.ByStartSequence)
                            .startSequence(startJSOffset)
                            .build()
                      )
                      .build()
                    var success = false
                    var orderedSub:JetStreamSubscription = null;
                    while(!success) {
                        breakable {
                            try {
                                this.logger.debug(s" Thread ${thread.getName()} trying for connection.")
                                val js = NatsConfigSource.config.js
                                orderedSub = js.subscribe(">", opso)
                            } catch {
                                case e: IOException => {
                                    Thread.sleep(1000L) 
                                    this.logger.debug(s" Thread ${thread.getName()} waiting for connection.")
                                    break
                                }
                            }
                        }
                        success = true
                    }

                    var currentSeqNumber: Long = startJSOffset
                    val buffer: ListBuffer[Message] = ListBuffer.empty[Message]
                    // there may be no (zero) messages in the partition
                    if(endJSOffset > 0) {
                        do {
                        var message:Message = null
                        while(message == null) {
                            try {
                                message = orderedSub.nextMessage(0)
                            } catch {
                                case e: NullPointerException =>
                            }
                        }
                        buffer += message
                        currentSeqNumber = message.metaData().streamSequence()
                        this.logger.debug(s"CurrentSeqNumber: $currentSeqNumber")
                        } while(currentSeqNumber < endJSOffset)
                    }
                    orderedSub.unsubscribe()

                    addToSharedMessageBuffer(buffer)
                }
              }
            }).start())
    }

    private def doLatch(): Unit = {
        // If the thread fails, we want to assure it will countdown the latch once the thread restarts so to
        // release other threads. If it happens that the thread failed right after the countdown that is ok, 
        // as counting down from zero does nothing.
        this.latch.countDown()
        this.logger.debug(s"Thread: ${Thread.currentThread().getName()}  latch: ${this.latch.getCount()}")
        // wait for sync between all batch threads and batcher readiness.
        this.latch.await()
        this.logger.debug(s"Thread: ${Thread.currentThread().getName()} moved past wait.")

        // setup with one thread per worker/partition, plus one for bacher
        this.synchronized {
            if(this.latch.getCount() < 1) {
                this.latch = new CountDownLatch(this.numWorkers + 1)
            }
        }
    }

    private def registerThread(thread:Thread, kv:KeyValue): Int = {
        if(!this.partitionThreadMap.contains(thread)) {
            val partition = createThreadPartitionAffinity(thread, kv)
            this.partitionThreadMap+= (thread -> partition)
        }
        return this.partitionThreadMap(thread)
    }

    private def getStartAndEndOffsets(partition:Int): (Long, Long) = {
        val startOffset = this.startOffsetMap(partition)
        val endOffset = this.endOffsetMap(partition)
        return (startOffset, endOffset)
    }

    private def createThreadPartitionAffinity(thread:Thread, kv:KeyValue): Int = {
        var numWorkers = this.numWorkers
        var done = false
        var partition = -1
        while ((numWorkers >= 1) && !done) {
            partition = numWorkers - 1
            val threadNameBytes = thread.getName().getBytes()
            try {
                kv.create(partition.toString(), threadNameBytes)
            } catch {
                case e: Exception => //println(e.getMessage())
            }
            val readBackThreadNameBytes = kv.get(partition.toString()).getValue()
            if (threadNameBytes.deep == readBackThreadNameBytes.deep) {
                done = true
                this.logger.info(s"Partition $partition is owned by thread ${thread.getName()} ")
            } else {
                numWorkers = numWorkers - 1
            }
    }
    return partition.toInt
    }

    
    private def testPartitionThreadsSanity(): Unit = {
        this.partitionThreadMap.keys.foreach((t:Thread) => {
            if(!t.isAlive() || t.isInterrupted()) {
                t.run()
            }
        })
    }

    private def addToSharedMessageBuffer(localBuffer:ListBuffer[Message]): Unit = {
        this.synchronized {
            this.buffer.insertAll(this.buffer.size, localBuffer)
        }

    }

    private def printInfo(): Unit = {
        val var_array: Array[(String, String)] =
            this. sparkContext.getConf.getAllWithPrefix("spark.executor.id")

        val exec_id_str: (String, String) = var_array(0)
            println(s"Found executor env var ${exec_id_str._1} with value  ${exec_id_str._2}"
        )

        val javaArgList = ManagementFactory.getRuntimeMXBean().getInputArguments();
        val listIterator = javaArgList.iterator()
        val argSeq = JavaConverters.asScalaIteratorConverter(listIterator).asScala;
        argSeq.foreach(r => println(r))
    }
}

abstract class PartitionThread extends Runnable {
    //val logger:Logger = LoggerFactory.getLogger(classOf[PartitionThread])
    val logger:Logger = Logger.getLogger("nats.connector")
}