import org.apache.log4j.LogManager

class MinHeap(size: Int){

  private val minHeap : Array[Cluster] = new Array[Cluster](size)
  private var currentSize : Int = 0

  def getMinHeap: Array[Cluster] = {
    minHeap
  }
  /*
    getter for size
   */
  def getSize: Int = {
    currentSize
  }
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
    val temp : Cluster = minHeap(i)
    minHeap(i) = minHeap(j)
    minHeap(j) = temp

  }

  /*
    Inserts a cluster into the MinHeap
   */
  def insert(cluster: Cluster): Unit = {

    if (currentSize == 0){
      minHeap(0) = cluster
      return
    }

    // insert new cluster at the end
    currentSize += 1
    minHeap(currentSize) = cluster

    //traverse the heap to relocate new cluster in the proper position
    this.moveUp(currentSize)

  }

  /*
    Removes the min (root) element from the MinHeap
   */
  def extractMin(): Cluster = {

    // store the root value to return and find the new root
    val root : Cluster = minHeap(0)
    minHeap(0) = minHeap(currentSize) // move last element to the root
    minHeap(currentSize) = null // remove last element as it has been moved to the root
    currentSize -= 1
    this.moveDown(0)

    root
  }

  def heapify(index: Int): Unit = {

    val parent = this.parent(index)

    if(parent > 0 && (minHeap(parent).distanceFromClosest > minHeap(index).distanceFromClosest)) {
      this.moveUp(index)
    }
    else {
      this.moveDown(index)
    }
  }

  def moveUp(current: Int): Unit = {
    val parent = current/2

    if(minHeap(parent).distanceFromClosest > minHeap(current).distanceFromClosest){ //do swap
      this.swap(current, parent)
      this.moveUp(parent)
    }
  }

  def moveDown(current: Int) : Unit= {

    val left: Int = leftChild(current)
    val right: Int = rightChild(current)

    var min = {        // Compare with left child
      if(left <= currentSize && minHeap(left).distanceFromClosest < minHeap(current).distanceFromClosest) {
        left
      }
      else {
        current
      }
    }
    min = {            // Compare with right child
      if(right <= currentSize && minHeap(right).distanceFromClosest < minHeap(min).distanceFromClosest) {
        right
      }
      else {
        min
      }
    }

    if(min != current){     // if minimum is any of children
      this.swap(current, min)
      this.moveDown(min)
    }
  }

  /*
    Remove the cluster of the specific index from the MinHeap
   */
  def delete(cluster: Cluster): Cluster = {

    // take the index of the cluster with the given id
    var nodeIndex = minHeap.indexOf(cluster)

    // declare the best possible minimum as the distance for the cluster we want to remove
    // so as to move up the whole MinHeap
    val delClusterDistance : Double = scala.Double.MinValue
    var parentCluster = minHeap(this.parent(nodeIndex))

    //move up the whole MinHeap
    while ( nodeIndex != 0 && Utils.clusterDistance(parentCluster, parentCluster.closest) > delClusterDistance){
      this.swap(nodeIndex, this.parent(nodeIndex))
      nodeIndex = this.parent(nodeIndex)

      parentCluster = minHeap(this.parent(nodeIndex))
    }

    // the element we wanted to remove has now reached the root of the MinHeap
    // so extract the root
    this.extractMin()
  }

  def delete(index:Int):Unit = {
    minHeap(index) = minHeap(currentSize)
    currentSize-=1
    this.heapify(index)
  }

  override def toString: String = {
    var str : String = ""
    minHeap.foreach(str += _ + " ")
    str
  }

}