import org.apache.spark.SparkConf
import java.net.InetAddress
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.hadoop.fs.Path


object startt {
  def main(args: Array[String]): Unit = {
    println("hiii")
    val conf=new SparkConf().setAppName("SPARK SMigration");
   println("into main")
   val logger=Logger.getLogger("Migration.class")
   logger.info("into main")
   val sc=new SparkContext(conf)
   val sqlContext =new SQLContext(sc)
   val hc=new HiveContext(sc)
var coreSitexmlpath=conf.get("spark.coreSitexmlpath")
var hdfsSiteXmlpath=conf.get("spark.hdfsSiteXmlpath")
var hiveSiteXmlpath=conf.get("spark.hiveSiteXmlpath")
var conff =new Configuration()
   conff.addResource(new Path(coreSitexmlpath))
   conff.addResource(new Path(hdfsSiteXmlpath))
   conff.addResource(new Path(hiveSiteXmlpath))
println(InetAddress.getLocalHost())
 logger.info("ip is: "+InetAddress.getLocalHost())
  }
}