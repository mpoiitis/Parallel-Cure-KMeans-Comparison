case class Cluster (points: Array[Point], var id : Long = 0, var representatives: Array[Point] = null,
                var closest : Cluster = null, var meanPoint : Point = null, var distanceFromClosest: Double = 0) {

  override def toString: String = {
    "Cluster with id: " + this.id.toString + " Mean Point: " + this.meanPoint + " Distance from closest: " + distanceFromClosest
  }
}
