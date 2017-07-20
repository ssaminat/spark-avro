import org.apache.spark.SparkConf
import java.net.InetAddress
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.hadoop.fs.Path
import scala.collection.parallel.ParIterableLike.Foreach


object startt {
  def main(args: Array[String]): Unit = {
    println("hiii")
    val conf=new SparkConf().setAppName("SPARK SMigration");
   println("into mainn")
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
 //hc.sql("use dev_tmd")
 hc.sql("use dev_ops")
 val tablesListtemp=hc.sql("show tables")
 val tablesList=tablesListtemp.select("tableName")
 // dd.filter("tableName like 'scb%'").foreach(x=>println(x))
 //val list=tablesList.filter("tableName like 'scb%'").collect
 val list=tablesList.filter("tableName like '%tsdev_all_rowcounts%'").collect
 for(lis <- list)
 {
   var table =lis.mkString
   println("processing started for "+table)
   //logger.info("processing started for "+table)
   var show_part_query="show partitions "+ table
    println("show partitions query "+show_part_query)
   val partition_List=hc.sql(show_part_query).collect
   for(x <-partition_List)
   {
   //var temp=table+"_tempdf"
   //rcds=2016_06_13
   var x1=x.mkString.split("=")(1)
   println("x1 "+x1)
   var query="select * from "+table+" where rcds = '"+x1+"'"
   println("select part query "+query) 
   val df=hc.sql(query)
   println("hey")
   var basepath="test12345/insert/testtt/"
   //hdfs://nnscbhaasdev/dev/scudee/
   var destpath="hdfs://nnscbhaasdev/dev/scudee/test10/"
   var path=basepath+table+"/"+x.mkString
   var dest_path=destpath+table
   df.save(path)
   // println(temp)
   }
 }
 /*list.forEach{x=>
    println(x)
    var temp=x.mkString+"_tempdf"
    var query="select * from "+x.mkString+" limit 10"
    println("query is: "+query)
  var df=hc.sql(query)
  
   println(temp)
   }*/
  }
}
