case class Cluster (points: Array[Point], var id : Int = 0, var representatives: Array[Point] = null,
                var closest : Cluster = null, var meanPoint : Point = null) {

  override def toString: String = "id: " + this.id.toString + " distance from closest: " + Utils.clusterDistance(this, this.closest)
}
