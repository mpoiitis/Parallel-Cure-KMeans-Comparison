/**
  * Created by Zaikis Dimitrios, 8 and Poiitis Marinos, 17 on 13/05/2019.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[*]").appName("BigDataApp").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import ss.implicits._


//    val files = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".txt"))
//
//    var data: DataFrame = null
//    for (file <- files) {
//      val fileDf= ss.read.csv(file.toString)
//      if (data!= null) {
//        data= data.union(fileDf)
//      } else {
//        data= fileDf
//      }
//    }
//
//    val numOfExamples = data.count()
//    val ratio = 0.01
//    data = data.sample(ratio)
//
//    println(numOfExamples)
//    println(data.count())

    val data: DataFrame = ss.read.option("inferSchema","true").csv("data/data1.txt").toDF("x", "y")

    Cure.run(data, 5, 5, ss)

//    val p1 = Point(Array(30, 40))
//    val p2 = Point(Array(5, 25))
//    val p3 = Point(Array(70, 70))
//    val p4 = Point(Array(10, 12))
//    val p5 = Point(Array(50, 30))
//    val p6 = Point(Array(35, 45))
//
//    val dummy = Cluster(Array(p1), id = -1)
//    val c = Cluster(Array(p3), closest = dummy)
//    val c2 = Cluster(Array(p4, p5, p6), closest = c, id = 2)
//    val c1 = Cluster(Array(p1, p2), closest = c2, id = 1)
//
//    p1.cluster = c1
//    p2.cluster = c1
//    p3.cluster = c
//    p4.cluster = c2
//    p5.cluster = c2
//    p6.cluster = c2

//    // KD Tree Test
//
//    var root = Node(p1, null, null)
//    val tree = new KdTree(root, k = 2)
//
//    tree.insert(p2)
//    tree.insert(p3)
//    tree.insert(p4)
//    tree.insert(p5)
//    tree.insert(p6)

//    // KD Tree Search Test
//    println(tree.search(p3))
//    println(tree.search(Point(Array(23, 222))))
//    // KD Tree Find Minimum Test
//    println("Minimum of 0'th dimension is " + tree.findMinimum(root, 0).point.dimensions(0))
//    println("Minimum of 1'th dimension is " + tree.findMinimum(root, 1).point.dimensions(1))
//    // KD Tree Deletion Test
//    root = tree.delete(root.point)
//    println("Root after deletion of (30, 40)")
//    println(root.point.dimensions(0) +", " + root.point.dimensions(1))

//    // KD Tree Closest Point of Different Cluster Test
//    println("Closest point to (30, 40): " + tree.closestClusterPoint(p1))

//    // Min Heap Test
//
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
