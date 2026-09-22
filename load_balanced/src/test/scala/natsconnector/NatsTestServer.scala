package natsconnector

import io.nats.{NatsRunnerUtils, NatsServerRunner}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.logging.Level

/** Runs a real JetStream-enabled nats-server for the duration of a suite, the same way the
  * nats.java test suite does (io.nats:jnats-server-runner). The server listens on a random free
  * port with a throwaway store directory, so the tests neither need nor disturb whatever may
  * already be listening on localhost:4222.
  *
  * The `nats-server` executable is looked up on the PATH, or at the absolute path given by the
  * `nats_server_path` environment variable.
  */
trait NatsTestServer extends BeforeAndAfterAll { this: Suite =>

  private var server: Option[NatsServerRunner] = None

  val natsHost: String = "localhost"

  def natsPort: String =
    server.map(_.getNatsPort.toString).getOrElse(
      throw new IllegalStateException("The test NATS server is not running"))

  /** The options every source needs to connect to the test server, plus/overridden by `extra`. */
  def natsParams(extra: (String, String)*): Map[String, String] =
    Map(
      "nats.host" -> natsHost,
      "nats.port" -> natsPort,
      "nats.stream.name" -> "TestStream",
      "nats.stream.subjects" -> "test.>",
      "nats.msg.ack.wait.secs" -> "60"
    ) ++ extra

  override protected def beforeAll(): Unit = {
    NatsTestServer.init()
    server = Some(
      try {
        NatsServerRunner.builder().jetstream(true).build()
      } catch {
        case e: Exception =>
          throw new IllegalStateException(
            s"Could not start '${NatsRunnerUtils.getResolvedServerPath}': these tests need a " +
              "nats-server executable on the PATH, or its absolute path in the " +
              s"'${NatsRunnerUtils.NATS_SERVER_PATH_ENV}' environment variable", e)
      })
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      server.foreach(_.close())
      server = None
    }
  }
}

object NatsTestServer {
  // Runner-wide defaults: keep the server's log out of the test output, and give a slow CI
  // machine enough time to get the server up before giving up on it.
  private lazy val initialized: Unit = {
    NatsRunnerUtils.setDefaultOutputLevel(Level.WARNING)
    NatsRunnerUtils.setDefaultConnectValidateTries(10)
    NatsRunnerUtils.setDefaultConnectValidateTimeout(200)
  }

  def init(): Unit = initialized
}
