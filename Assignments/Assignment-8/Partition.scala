//Name: Samarth Manjunath
//UTA ID: 1001522809
//Subject: Advanced Database Systems
import org.apache.spark.graphx.{Graph => Graph1, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Partition {
def main(args:Array[String])
{
 val config_entity = new SparkConf().setAppName("Partition")
 val scan_entity = new SparkContext(config_entity)
 var count_entity = -1.toLong
 val edges: RDD[Edge[Long]] = scan_entity.textFile(args(0)).map( line => { val (node, neighbours) = line.split(",").splitAt(1)
 												(node(0).toLong,neighbours.toList.map(_.toLong)) } )
												.flatMap( x => x._2.map(y => (x._1, y)))
												.map(nodes => Edge(nodes._1, nodes._2, nodes._1))
val new_graph : Graph1[Long,Long] = Graph1.fromEdges(edges, "defaultProperty") .mapVertices(
          (id, _) => { count_entity = count_entity+1
          if(count_entity<5) id else -1.toLong
        })

      val preg_entity = new_graph.pregel((-1).toLong, 6)(
        (id, grp, newgrp) => math.max(grp, newgrp).toLong,
        triplet => {
          if (triplet.dstAttr == (-1).toLong) {
            Iterator((triplet.dstId, triplet.srcAttr ))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.max(a, b)
      )

  val result_entity = preg_entity.vertices.map(x => (x._2, 1)).reduceByKey(_+_).sortByKey().map(g => g._1.toString + " " +g._2.toString )
  result_entity.collect().foreach(println)
}
}
