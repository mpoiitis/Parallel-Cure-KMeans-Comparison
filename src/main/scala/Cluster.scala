case class Cluster (points: Array[Point], var id : Long = 0, var representatives: Array[Point] = null,
                var closest : Cluster = null, var meanPoint : Point = null, var distanceFromClosest: Double = Double.MaxValue) {

  points.foreach(_.cluster = this)
  if (representatives != null) representatives.foreach(_.cluster = this)
  if (meanPoint != null) meanPoint.cluster = this

  override def toString: String = {
    "Cluster with id: " + this.id.toString + " points: " + points.length + " reps: " + representatives.length
  }
}
