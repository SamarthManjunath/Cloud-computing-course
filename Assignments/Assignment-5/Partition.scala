/*
Name: Samarth Manjunath
UTA ID: 1001522809
Assignment-5
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  var sample_number = 1
  var a = 0
  val negative_neg:Long = -1
  val deep_length = 6

  def Vertical_function(str:Array[String]): (Long,Long,List[Long]) ={
    var id_of_centroid:Long = -1
    if(sample_number<=10){
    	id_of_centroid = str(0).toLong
        sample_number = sample_number+1
    }
    (str(0).toLong,id_of_centroid,str.tail.map(_.toString.toLong).toList)
  }

  def reduce_function(vertex: (Long,Iterable[(Either[(Long,List[Long]),Long])])):(Long,Long,List[Long])={
    var clster:Long = -1
	var adcnt:List[Long] = List[Long]()
    for (p <- vertex._2){
      p match{
        case Right(c) => {clster=c}
        case Left((c,adj)) if (c>0)=> return (vertex._1,c,adj)
        case Left((negative_neg,adj)) => adcnt=adj
      }
    }
    return (vertex._1,clster,adcnt)
  }

   def method_one(vert:(Long,Long,List[Long])):List[(Long,Either[(Long,List[Long]),Long])]={
      var temporary_one = List[(Long,Either[(Long,List[Long]),Long])]()
      if(vert._2>0) {vert._3.foreach(x => {temporary_one = (x,Right(vert._2))::temporary_one})}
      temporary_one = (vert._1,Left(vert._2,vert._3))::temporary_one
    temporary_one
  }

  def main ( args: Array[ String ] ) :Unit = {
    val conf = new SparkConf().setAppName("New_Partition")
    val sc = new SparkContext(conf)
    var graph = sc.textFile(args(0)).map(line => Vertical_function(line.split(",")))
    for (i <- 1 to deep_length){
      graph = graph.flatMap(vertex => method_one(vertex)).groupByKey().map(vertex => reduce_function(vertex))
    }
    var final_output = graph.map(h => (h._2,1))
    var final_result = final_output.reduceByKey(_+_).collect()
    final_result.foreach(println)
  }
}
