case class Cluster (points: Array[Point], var id : Long = 0, var representatives: Array[Point] = null,
                var closest : Cluster = null, var meanPoint : Point = null, var distanceFromClosest: Double = Double.MaxValue) {

  override def toString: String = {
    "Cluster with id: " + this.id.toString
  }
}
