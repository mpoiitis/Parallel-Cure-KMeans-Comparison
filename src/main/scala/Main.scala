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

    val p1 = Point(Array(2.2, 2.7))
    val p2 = Point(Array(2.0, 3.0))
    val p3 = Point(Array(5.0, 2.5))
    val p4 = Point(Array(1.0, 3.0))

    val dummy = Cluster(Array(p1), id = -1)
    val c = Cluster(Array(p3), closest = dummy)
    val c2 = Cluster(Array(p3, p4), closest = c, id = 2)
    val c1 = Cluster(Array(p1, p2), closest = c2, id = 1)

  }
}
