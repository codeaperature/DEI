package dei

import javax.servlet.Servlet

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet,
  ServletContextHandler, ServletHolder}

import scala.util.Try

import java.lang.String
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result

import java.io.{DataOutputStream, ByteArrayOutputStream}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.spark.{rdd, SparkContext, SparkConf}
import org.apache.hadoop.hbase.client.Result

object App {

  var skillsList = Array[String]()

  def main(args : Array[String]) {
    // Parse args
    val DEFAULT_RESOURCE_ROOT = System.getProperty("user.dir") + "/static"
    println(DEFAULT_RESOURCE_ROOT)
    val DEFAULT_PORT = 8089
    val USAGE =
      s"""USAGE: mvn exec:java
        |
        |# PORT=80, RESOURCE ROOT=/path/dir
        |mvn exec:java -Dexec.mainClass="dei.App" -Dexec.args="80 /path/dir"
        |
        |spark-submit --class "dei.App" --master local[4] ~/WebRunner/target/ScalatraDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
        |
        |# PORT=$DEFAULT_PORT, RESOURCE ROOT=$DEFAULT_RESOURCE_ROOT
        |USAGE: mvn exec:java -Dexec.mainClass="dei.App"
      """.stripMargin.trim
    if (args.length != 0 && Try(args(0).toInt).isFailure) {
      System.err.println(USAGE)
      System.exit(1)
    }
    val (port, root) = args match {
      case Array(port,root) => (port.toInt,  root)
      case Array(port)      => (port.toInt,   DEFAULT_RESOURCE_ROOT)
      case _                => (DEFAULT_PORT, DEFAULT_RESOURCE_ROOT)
    }

    // Create server
    System.setProperty("org.eclipse.jetty.LEVEL","INFO")
    val server = new Server(port)


    // Register RealtimeServlet for demo 2
    registerServlet(server,
      resourceRoot = root,
      servletName = "app",
      servletUrlPath = "/app/*",
      servlet = new JobFinderServlet())

    // Start server
    server.start()
    server.join()
  }
  def registerServlet(server: Server,
                      resourceRoot:String,
                      servletName:String,
                      servletUrlPath: String,
                      servlet: Servlet): Unit = {

    // Setup paths

    // Change resourcesPath to hard-coded value

    // Setup application "context" (handler tree in jetty speak)
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setResourceBase(resourceRoot)
    println("resourceRoot="+resourceRoot)

    // Path in URL to match
    context.setContextPath("/")
    server.setHandler(context)

    // Add custom servlet
    val holderDynamic = new ServletHolder(servletName, servlet)
    context.addServlet(holderDynamic, servletUrlPath)

    // Default servlet for root content (always last)
    val holderPwd = new ServletHolder("default", new DefaultServlet())
    holderPwd.setInitParameter("dirAllowed", "true")
    context.addServlet(holderPwd, "/")
  }

}
