/*
export SPARK_PKGS=$(cat << END | xargs echo | sed 's/ /,/g'
org.apache.hadoop:hadoop-aws:2.7.1
com.amazonaws:aws-java-sdk-s3:1.10.30
com.databricks:spark-csv_2.10:1.3.0
com.databricks:spark-avro_2.10:2.0.1
org.apache.spark:spark-streaming-twitter_2.10:1.5.2
org.twitter4j:twitter4j-core:4.0.4
org.apache.hbase:hbase-client:1.1.2
org.apache.hbase:hbase-common:1.1.2
END
)

The following does not work in my local REPL:
com.amazonaws:aws-java-sdk-s3:1.10.44

*/

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.feature.Normalizer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.lang.String
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

/*
to do 
cd $SPARK_HOME/conf
cp log4j.properties.template log4j.properties
Edit log4j.properties.

Replace rootCategory=INFO with rootCategory=WARN
*/


def convertScanToString(scan: Scan): String = {
  val proto = ProtobufUtil.toScan(scan)
  Base64.encodeBytes(proto.toByteArray())
}

val path = System.getenv("HOME") + "/.ssh/aws-hadoop-conf.xml"
sc.hadoopConfiguration.addResource((new java.io.File(path)).toURI().toURL())
sc.hadoopConfiguration.get("fs.s3a.access.key")
sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")


@transient val conf = HBaseConfiguration.create()
val path = System.getenv("HOME") + "/conf/hbase-site.xml"
conf.addResource((new java.io.File(path)).toURI().toURL())


val scan = new Scan()
scan.setCaching(500)
scan.setCacheBlocks(false)
conf.set(TableInputFormat.INPUT_TABLE, "skills")
conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "v1")
conf.set(TableInputFormat.SCAN, convertScanToString(scan))
val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

val sks2 = rdd.map{case (k,v) => new String(k.copyBytes)}
// val sks3 = rdd.map{case (k,v) => new String(k.copyBytes)}.filter(text => text.length > 0)



/*
val csv = sc.textFile("skills/skills.csv")
val sks = csv.map(_.split(",")).
  map(_(0).toLowerCase).
  filter(_.length != 0) 
val bcsk = sc.broadcast(sks.collect)
*/
val bcsk = sc.broadcast(sks2.collect)

// gets the file with tuples (filename, content/desc)
val wtf = sc.wholeTextFiles("s3n://opps/jobs_sof").cache
wtf.persist
// wtf.take(5).foreach(println)
//val s3 = wtf.map(x => x._1)
//val desc = wtf.map(x => x._2) 
// desc.take(5).foreach(println)
// Extract doc names: Remove directories from paths to keep file name.
val docNames = wtf.keys.map(_.replaceAll("^.*\\/",""))

//docNames.take(5).foreach(println) 

/*
change c++ to cpp 
change c# to cs
seek for node.js 
*/

// Extract docs: Split up the text of the files into words.


val docs = wtf.values.
  map(text => { 
    var i = 0
    var j = 0
    var cc = 0
    var cnt = Array[String]()
    var copyText = text.toLowerCase
    for(i <- 0 until bcsk.value.size) {
      var searchText = bcsk.value(i) 
      if((searchText == "apache-spark") ||
        (searchText == "apache-kafka") ||
        (searchText == "apache-storm")) {
        searchText = searchText.replaceAll("\\Qapache-\\E", "")
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ bcsk.value(i) 
        }
      }

      if(searchText.contains('-')) {
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ bcsk.value(i) 
        }
        if(cc > 0) {
          copyText = copyText.replaceAll("\\Q" + bcsk.value(i) + "\\E", "")
        }
        // now look for same text without '-'
        searchText = searchText.replace('-', ' ')
        cc = searchText.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ bcsk.value(i) 
        }
        if(cc > 0) {
          copyText = copyText.replaceAll( "\\Q" + bcsk.value(i) + "\\E", "")
        }
      } // if search text with a '-'

      if((searchText == "c++") ||
        (searchText == "c#") ||
        (searchText == "f#")) {
        var searchTextCopy = "\\Q"+searchText + "\\E"
        cc = searchTextCopy.r.findAllIn(copyText).length
        for(j <- 0 until cc) {
          cnt = cnt :+ bcsk.value(i) 
        }
      }      
    } // for
    var words = copyText.split("\\W+")
    for(i <- 0 until bcsk.value.size) {
      for(j <- 0 until words.size) {
          if(words(j) == bcsk.value(i)) {
              cnt = cnt :+ bcsk.value(i)
          }
      }
    }
    cnt  
  }).
  map(x => x.toSeq)


  // HashingTF calculates term frequencies.
val hashingTF = new HashingTF()

// Get term frequencies.
val tf:RDD[Vector] = hashingTF.transform(docs)
tf.cache()


// normalize
val normalizer = new Normalizer()


// Get TF-IDF.
val idf = new IDF(minDocFreq = 1).fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf).map(x => (normalizer.transform(x)))

// Define search term.
val searchVector = normalizer.transform(hashingTF.transform(Seq("java","c++", "c#")))
// Find match scores by doing dot product with all.
val searchMul = new ElementwiseProduct(searchVector)
val scores = searchMul.transform(tfidf).map(v => v.toSparse.values.sum)
docNames.zip(scores).sortBy((x => x._2),false).map(x => x._1 +"\t"+x._2).take(10).foreach(println)

val fs:FileSystem = FileSystem.get(new URI("Skills_TFIDF_Docs"), sc.hadoopConfiguration)
fs.delete(new Path("Skills_TFIDF_Docs"), true) // true for recursive

val fs:FileSystem = FileSystem.get(new URI("Skills_TFIDF_Xform"), sc.hadoopConfiguration)
fs.delete(new Path("Skills_TFIDF_Xform"), true) // true for recursive


tfidf.saveAsObjectFile("Skills_TFIDF_Xform")
docNames.saveAsObjectFile("Skills_TFIDF_Docs")

/*

val normalizerLoad = new Normalizer()

// to load
val ld_tfidf:RDD[org.apache.spark.mllib.linalg.Vector] = sc.objectFile("Skills_TFIDF_Xform")
val ld_dn:RDD[String] = sc.objectFile("Skills_TFIDF_Docs")

// Define search term.
val hTF = new HashingTF()
val sVector = normalizerLoad.transform(hTF.transform(Seq("java", "java", "scala","spark")))
// Find match scores by doing dot product with all.
val sMul = new ElementwiseProduct(sVector)
val scores2 = sMul.transform(ld_tfidf).map(v => v.toSparse.values.sum)

ld_dn.zip(scores2).sortBy((x => x._2),false).map(x => x._1 +"\t"+x._2).take(10).foreach(println)

*/

/*
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

val fs:FileSystem = FileSystem.get(new URI("Skills_TFIDF_Docs"), sc.hadoopConfiguration)
fs.delete(new Path("Skills_TFIDF_Docs"), true) // true for recursive

val fs:FileSystem = FileSystem.get(new URI("Skills_TFIDF_Xform"), sc.hadoopConfiguration)
fs.delete(new Path("Skills_TFIDF_Xform"), true) // true for recursive


*/

// run with -i 
System.exit(0)
