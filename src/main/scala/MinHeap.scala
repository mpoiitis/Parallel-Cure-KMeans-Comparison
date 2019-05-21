class MinHeap(size: Int){

  var minHeap : Array[Cluster] =new Array[Cluster](size)
  var currentSize : Int = -1

  /*
    Get the parent of the cluster on the given index
   */
  def parent(i: Int) : Int = {
    (i-1) / 2
  }

  /*
    Get the left child of the cluster on the given index
   */
  def leftChild(i: Int) : Int = {
    2*i + 1
  }

  /*
    Get the right child of the cluster on the given index
   */
  def rightChild(i: Int) : Int = {
    2*i + 2
  }

  /*
    Swap two elements in the heap array
   */
  def swap(i: Int, j: Int): Unit ={
    val temp : Cluster = this.minHeap(i)
    this.minHeap(i) = this.minHeap(j)
    this.minHeap(j) = temp

  }

  def insert(cluster: Cluster): Unit = {
    if (currentSize == size) {
      println("Overflow: Could not insert key!")
      return
    }

    // insert new cluster at the end
    currentSize += 1
    var i : Int = currentSize - 1
    minHeap(i) = cluster

    // fix min heap property if it is violated
    var parentCluster = this.minHeap(this.parent(i))
    var childCluster = this.minHeap(i)
    while (i !=0 &&  Utils.clusterDistance(parentCluster, parentCluster.closest)> Utils.clusterDistance(childCluster, childCluster.closest) ){
      this.swap(i, this.parent(i))
      i = this.parent(i)

      parentCluster = this.minHeap(this.parent(i))
      childCluster = this.minHeap(i)
    }
  }


}
