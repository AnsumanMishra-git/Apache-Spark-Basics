import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object Assignment1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","assignment1")
  val rdd1 = sc.textFile("C:/Users/hp/Documents/Data Engineering/Dataset/rdd.dataset1")
  rdd1.collect.foreach(println)
  val rdd2 = rdd1.map(row=>{
    val fields = row.split(",")
    if(fields(1).toInt>18)
      (fields(0),fields(1),fields(2),"Y")
    else
      (fields(0),fields(1),fields(2),"N") 
  })
  rdd2.collect.foreach(println)
}
