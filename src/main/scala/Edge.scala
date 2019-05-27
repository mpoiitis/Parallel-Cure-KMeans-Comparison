case class Edge(left: Int, right: Int, weight: Double) {
  override def toString: String = "[(" + this.left + ", " + this.right + ") " + this.weight + "]"
}
