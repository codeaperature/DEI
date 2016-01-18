package dei

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{rdd, SparkContext, SparkConf}
import org.scalatra.ScalatraServlet
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.Normalizer

class JobFinderServlet extends ScalatraServlet {
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }


  val spconf = new SparkConf().setAppName("Skill-Matcher Application")
  val sc = new SparkContext(spconf)
//  val skillsList = Array("c++", "scala", "c#", "hbase", "java", "javascript", "spark", "apache-kafka")
@transient val conf = HBaseConfiguration.create()
  val path = System.getenv("HOME") + "/conf/hbase-site.xml"
  conf.addResource((new java.io.File(path)).toURI().toURL())

  val scan = new Scan()
  scan.setCaching(500)
  scan.setCacheBlocks(false)
  conf.set(TableInputFormat.INPUT_TABLE, "skills")
  conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "v1")
  conf.set(TableInputFormat.SCAN, convertScanToString(scan))
  val skillsRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

  val skillsList = skillsRDD.map{case (k,v) => new String(k.copyBytes)}.collect


  val magic =
  """
    |Skills Summary    Programming Languages Experience (Ployglot): C/C++/C# (VS & GNU) Java Javascript / AJAX / XML / JSON Golang Scala Perl PHP Assembly Kafka   Big Data and Database: Spark, Hadoop DFS, Map/Reduce, Solr, Storm, AWS, HBase, Hive, Pig, SQL, Postgres, SQLite   Engineering Tools: Wireshark, tcpdump, wget, Curl, Netcat, Apache Bench, Charles Proxy, SOAP UI Firefox/Chrome (plug-ins ex: Web development, headers, Firebug, GreaseMonkey …) Winscp, Putty, Secure CRT, ssh/scp Cygwin (sshd, gcc, g++, X-Windows, Bash …) Visual Studio, Eclipse, ADB   Operating Systems: Windows, Linux/*nix, OS/X, DOS, CP/M, RTOS, Android   Productivity Applications: MS Office/Project/PPT, source control (Perforce, CVS), compression (zip, tar, pdf) Remote Desktop, WebEx, VNC, CIPE, XTerm, VPN, NX Machine Hardware Productivity Tools: Oscilloscope, Logic / Spectrum Analyzer, Spectrophotometer, DVM, Waveform Generator Professional Services, Sales, Marketing & Project Management: Strong written and verbal communications Sales Support Partner Implementations Internationally adept (customs & cultural sensitivity) World and U.S. travel experience Product rollout plans (engineering & marketing) Project & team coordination Multitask / handle multiple projects and customers Present / train (& prepare) technical and non-technical groups Product requirements / specifications, change requests and design proposals Create / contribute sales / support / marketing material (app notes, FAQs, brochures, manuals …) Excellent customer service and ‘customer farming’ skills Comprehensive understanding of business systems and procedures Business / product opportunity plans Product launch / lifecycle plans   Foreign Languages: Rudimentary Swedish Hobbies & Interests: Cooking, Blues / Ballroom Dancing, Exercise, Tech Blogs, Software Programming Languages & Algorithms, Puzzles, & Coding Challenges
  """.stripMargin


  before() {
    contentType = "text/html"
  }
// change action to hello or bye and  the posted data will go to that page
  get("/") {
    <div>
      <h1>What Up Resume?</h1>
      <h2>Upload your resume to find the jobs that match!</h2>
      Paste a text copy your resume in the box below.
      <form action="/app/getMatch" method="post">
        <textarea name="resume" COLS="80" ROWS="20"></textarea>
        <br/>
        <input type="submit" value="Find Jobs!"/>
      </form>
      <br>
      <div id ="notice">Note: Your resume may be captured in the logs.</div>
      </br>
    </div>
  }

  post("/getMatch") {
    var resumebox = ""
    if(params.get("resume").getOrElse("No Resume Loaded") == "magic") {
      resumebox = magic
    }
    else {
      resumebox = params.get("resume").getOrElse("No Resume Loaded")
    }
    var i = 0
    var j = 0
    var cc = 0
    var cnt = Array[String]()
    var copyText = resumebox.toLowerCase
    println("list cnt: "+ skillsList.size)
    for(i <- 0 until skillsList.size) {
      println("Start Skill = " + skillsList(i))
      var searchText = skillsList(i)
      if((searchText == "apache-spark") ||
        (searchText == "apache-kafka") ||
        (searchText == "apache-storm")) {
        searchText = searchText.replaceAll("\\Qapache-\\E", "")
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ skillsList(i)
        }
        printf("apache-: cc = %d, skill = %s, cnt = %d\n", cc, searchText, cnt.size)
      }
      if(searchText.contains('-')) {
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ skillsList(i)
        }
        printf("has -: cc = %d, skill = %s, cnt = %d\n", cc, searchText, cnt.size)
        if(cc > 0) {
          copyText = copyText.replaceAll("\\Q" + skillsList(i) + "\\E", "")
        }
        // now look for same text without '-'
        searchText = searchText.replace('-', ' ')
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ skillsList(i)
        }
        printf("has -: cc = %d, skill = %s, cnt = %d\n", cc, searchText, cnt.size)
        if(cc > 0) {
          copyText = copyText.replaceAll("\\Q" + skillsList(i) + "\\E", "")
        }
      } // if search text with a '-'
      if((searchText == "c++") ||
        (searchText == "c#") ||
        (searchText == "f#")) {
        var searchTextCopy = "\\Q"+searchText + "\\E"
        cc = searchTextCopy.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ skillsList(i)
        }
        printf("has ++ or #: cc = %d, skill = %s, cnt = %d\n", cc, searchText, cnt.size)
      }

    } // for
    var words = copyText.split("\\W+")
    println("size of words = " + words.size)
    for(i <- 0 until skillsList.size) {
      for(j <- 0 until words.size) {
        if(words(j) == skillsList(i)) {
          cnt = cnt :+ words(j)
        }
      }
    }

    println("Size of cnt = "+ cnt.size)

    for(i <- 0 until cnt.length) {

      println(i + " - " + cnt(i))
    }

    val ld_tfidf:RDD[org.apache.spark.mllib.linalg.Vector] = sc.objectFile("Skills_TFIDF_Xform")
    val ld_dn:RDD[String] = sc.objectFile("Skills_TFIDF_Docs")

    // Define search term.
    val normalizer = new Normalizer()
    val hTF = new HashingTF()
//    val sVector = hTF.transform(Seq("java", "java", "scala","spark"))
    val sVector = normalizer.transform(hTF.transform(cnt.toSeq))
    // Find match scores by doing dot product with all.
    val sMul = new ElementwiseProduct(sVector)
    val scores2 = sMul.transform(ld_tfidf).map(v => v.toSparse.values.sum)

    val results = ld_dn.zip(scores2).sortBy((x => x._2),false).map(x => x._1 +"\t"+x._2).take(25)
    results.foreach(println)
    var strRes = "<div id=\"innerResult\">"
    for(i <- 0 until results.size) {
      val jobURL = results(i).split("\t")
      strRes = strRes +
//        "<div>http://careers.stackoverflow.com/jobs/"+jobURL(0).replace('_', '/') +"</div>\n"
        "<div><a href=\"http://careers.stackoverflow.com/jobs/" +
        jobURL(0).replace('_', '/') +"\" target=\"_blank\">"+
        "http://careers.stackoverflow.com/jobs/"+jobURL(0).replace('_', '/')+"</a></div>\n"
    }
    strRes = strRes + "</div>"
    //      <a href="#YourAnchor">blabla</a>

    println(resumebox)
    <div id ="mainsection">
      <h1>What Up Resume?</h1>
      <h2>Based on your skills, here are the jobs that match your resume:</h2>
      <div id ="results">{scala.xml.XML.loadString(strRes)}</div>
      <br>
        <div id ="notice">Note: Some job listings may have expired.</div>
      </br>
    </div>
  }
/*
  post("/hello") {
    val user = params.get("user").getOrElse("Unknown")
    <div>Hello {user}!</div>
  }

  post("/bye") {
    val user = params.get("user").getOrElse("Unknown")
    <div>Goodbye {user}!</div>
  }
*/
  notFound {
    serveStaticResource() getOrElse resourceNotFound()
  }
}
