import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.math.min


object Asssignment2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","asssignment2")
  val input = sc.textFile("C:/Users/hp/Documents/Data Engineering/Dataset/tempdata.csv")
  val parsed = input.map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3)))
  val filtered = parsed.filter(x=>x._3=="TMIN")
  val tuple = filtered.map(x=>(x._1,x._4.toInt))
  val reduced = tuple.reduceByKey((x,y)=>min(x,y))

  reduced.collect.foreach(println)
}
