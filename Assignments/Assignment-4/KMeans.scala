/*
Name: Samarth Manjunath
UTA ID: 1001522809
Subject: Advanced Database Systems.
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()



  def length (p: Point, point: Point): Double ={

    var sample1=p._1 - point._1
    var sample2=p._2 - point._2
    var length = Math.sqrt (sample1 * sample1 + sample2 * sample2 );
    length
  }

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val alpha = line.split(",")
      (alpha(0).toDouble,alpha(1).toDouble)}).collect

    var dots=sc.textFile(args(0)).map(line=>{val beta=line.split(",")
      (beta(0).toDouble,beta(1).toDouble)})

    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = dots.map { p => (cs.value.minBy(length(p,_)), p) }
        .groupByKey().map { case(c,pointva)=>
        var counter=0
        var sampleX=0.0
        var sampleY=0.0

        for(position <- pointva) {
           counter += 1
           sampleX+=position._1
           sampleY+=position._2
        }
        var cx=sampleX/counter
        var cy=sampleY/counter
        (cx,cy)

      }.collect
    }

centroids.foreach(println)
    }
}