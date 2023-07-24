
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object w11_p2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConfig = new SparkConf()
    .set("spark.app.name","my App")
    .set("spark.master","local[2]")
    
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  import spark.implicits._
  
  case class windowData(Country: String, Weeknum: Int, NumInvoices: Int, TotalQuantity: Int,InvoiceValue: Double)
  
  val data = spark.sparkContext.textFile("C:/Users/hp/Documents/Data Engineering/windowdata.csv")
  
  val split = data.map(_.split(","))
  val mapped = split.map(x=>windowData(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toDouble))

  val df = mapped.toDF().repartition(8)
  df.show

  df.write.
  format("json").
  mode(SaveMode.Overwrite).
  option("path","C:/Users/hp/Documents/Data Engineering/solution2").
  save()
  
  spark.stop()
}
