
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame , Dataset}
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.types.{IntegerType,StringType,StructType}
import org.apache.spark.sql.SaveMode


object w11_p1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConfig = new SparkConf()
    .set("spark.app.name","my App")
    .set("spark.master","local[2]")
    
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  import spark.implicits._
  
  val schema = new StructType()
  .add("country",StringType,true)
  .add("weeknum",IntegerType,true)
  .add("numinvoices",IntegerType,true)
  .add("totalquantity",IntegerType,true)
  .add("invoicevalue",IntegerType,true)
  
  val data = spark.read
  .option("header",false)
  .option("inferSchema",false)
  .format("csv")
  .option("path","C:/Users/hp/Documents/Data Engineering/windowdata.csv")
  .schema(schema)
  .load()
  
  val df = data.toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")


  df.write.
  format("parquet").
  mode(SaveMode.Overwrite).
  partitionBy("Country", "weeknum").
  option("path","C:/Users/hp/Documents/Data Engineering/solution1").
  save()
  
  spark.stop()
}
