/**
  * Created by Zaikis Dimitrios, 8 and Poiitis Marinos, 17 on 13/05/2019.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Main {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[*]").appName("BigDataApp").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import ss.implicits._

    val p1 = Point(Array(30, 40))
    val p2 = Point(Array(5, 25))
    val p3 = Point(Array(70, 70))
    val p4 = Point(Array(10, 12))
    val p5 = Point(Array(50, 30))
    val p6 = Point(Array(35, 45))

    // KD Tree Test
    var root = Node(p1, null, null)
    val tree = new KdTree(root, k = 2)
    tree.insert(p2)
    tree.insert(p3)
    tree.insert(p4)
    tree.insert(p5)
    tree.insert(p6)

//    // KD Tree Deletion Test
//    root = tree.delete(root.point)
//    println("Root after deletion of (30, 40)")
//    println(root.point.dimensions(0) +", " + root.point.dimensions(1))
//    // KD Tree Search Test
//    println(tree.search(p3))
//    println(tree.search(Point(Array(23, 222))))
//    // KD Tree Find Minimum Test
//    println("Minimum of 0'th dimension is " + tree.findMinimum(root, 0).point.dimensions(0))
//    println("Minimum of 1'th dimension is " + tree.findMinimum(root, 1).point.dimensions(1))

//    // Min Heap Test
//    val dummy = Cluster(Array(p1), id = -1)
//    val c = Cluster(Array(p3), closest = dummy)
//    val c2 = Cluster(Array(p3, p4), closest = c, id = 2)
//    val c1 = Cluster(Array(p1, p2), closest = c2, id = 1)
//
//    println(c)
//    println(c1)
//    println(c2)

//    val minHeap = new MinHeap(11)
//
//    minHeap.insert(c)
//    minHeap.insert(c1)
//    minHeap.insert(c2)
//    println("=================")
//    println(minHeap)
//    println(minHeap.delete(0))
//    println(minHeap)
//    println("=================")
//    minHeap.insert(c)
//    println(minHeap)
//    println(minHeap.delete(0))
//    println(minHeap)
  }
}
