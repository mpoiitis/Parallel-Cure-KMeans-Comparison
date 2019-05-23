
class MinHeap(size: Int){

  private var minHeap : Array[Cluster] = new Array[Cluster](size)
  private var currentSize : Int = _

  def getMinHeap: Array[Cluster] = {
    minHeap
  }
  /*
    getter for size
   */
  def getSize: Int = {
    currentSize + 1
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
    if (currentSize == size) {
      throw new Exception("Overflow: Could not insert key!")
    }

    // insert new cluster at the end
    currentSize += 1
    var i : Int = currentSize - 1
    minHeap(i) = cluster

    // fix min heap property if it is violated
    var parentCluster = minHeap(this.parent(i))
    var childCluster = minHeap(i)
    while (i !=0 &&  Utils.clusterDistance(parentCluster, parentCluster.closest)> Utils.clusterDistance(childCluster, childCluster.closest) ){
      this.swap(i, this.parent(i))
      i = this.parent(i)

      parentCluster = minHeap(this.parent(i))
      childCluster = minHeap(i)
    }
  }

  /*
    Removes the min (root) element from the MinHeap
   */
  def extractMin(): Cluster = {
    if (currentSize <= 0){
      throw new Exception("Heap is already empty!")
    }
    if (currentSize == 1){
      currentSize -= 1
      return minHeap(0)
    }

    // store the root value to return and find the new root
    val root : Cluster = minHeap(0)
    minHeap(0) = minHeap(currentSize-1) // move last element to the root
    minHeap(currentSize-1) = null // remove last element as it has been moved to the root
    currentSize -= 1
    this.heapify(0)

    root
  }

  /*
    Heapify a subtree with the given index as root
    Assumes that the subtrees are already heapified
   */
//  def heapify(i: Int): Unit = {
//    val left : Int = this.leftChild(i)
//    val right : Int = this.rightChild(i)
//    var smallest : Int = i
//
//
//    // find the smallest between left, right children and current node
//    val leftCluster = minHeap(left)
//    val rightCluster = minHeap(right)
//    var parentCluster = minHeap(i)
//
//    if (left < currentSize &&
//      Utils.clusterDistance(leftCluster, leftCluster.closest) < Utils.clusterDistance(parentCluster, parentCluster.closest)){
//      smallest = left
//    }
//
//    if (right < currentSize &&
//      Utils.clusterDistance(rightCluster, rightCluster.closest) < Utils.clusterDistance(minHeap(smallest), minHeap(smallest).closest)){
//      smallest = right
//    }
//
//    // if smallest is different than the current node, swap them and recurse
//    if (smallest != i) {
//      this.swap(i, smallest)
//      heapify(smallest)
//    }
//  }
  def heapify(index: Int): Unit = {

    val parentI = index /2
    val lChild = index*2
    val rChild = lChild +1

    if(parentI > 0 && (minHeap(parentI).distanceFromClosest > minHeap(index).distanceFromClosest)) percolateUp(index)
    else percolateDown(index)
  }

  def percolateUp(curr: Int): Unit = {
    val pi = curr/2
    if(minHeap(pi).distanceFromClosest > minHeap(curr).distanceFromClosest){ //do swap
      val tmp =minHeap(pi)
      minHeap(pi)=  minHeap(curr)
      minHeap(curr) = tmp
      percolateUp(pi)
    }
  }

  def percolateDown(curr: Int) : Unit= {

    val lChild = curr*2
    val rChild = lChild +1

    var min = {        // Compare with left child
      if(lChild <= size && minHeap(lChild).distanceFromClosest < minHeap(curr).distanceFromClosest) lChild
      else curr
    }
    min = {            // Compare with right child
      if(rChild <= size && minHeap(rChild).distanceFromClosest < minHeap(min).distanceFromClosest) rChild
      else min
    }

    if(min != curr){     // if minimum is any of children
      val tmp = minHeap(min)
      minHeap(min) = minHeap(curr)
      minHeap(curr) = tmp
      percolateDown(min)
    }
  }

  /*
    Remove the cluster of the specific index from the MinHeap
   */
  def delete(clusterId: Long): Cluster = {

    // take the index of the cluster with the given id
    var nodeIndex = minHeap.indexWhere(_.id == clusterId)

    // declare the best possible minimum as the distance for the cluster we want to remove
    // so as to move up the whole MinHeap
    val delClusterDistance : Double = scala.Double.MinValue
    var parentCluster = minHeap(this.parent(nodeIndex))

    //move up the whole MinHeap
    while ( nodeIndex != 0 && Utils.clusterDistance(parentCluster, parentCluster.closest) > delClusterDistance){
      swap(nodeIndex, this.parent(nodeIndex))
      nodeIndex = this.parent(nodeIndex)

      parentCluster = minHeap(this.parent(nodeIndex))
    }

    // the element we wanted to remove has now reached the root of the MinHeap
    // so extract the root
    this.extractMin()
  }

  /*
    relocate an element in the MinHeap (delete and reinsert)
   */
  def relocate(cluster: Cluster): Unit = {
    this.delete(cluster.id)
    this.insert(cluster)
  }

  override def toString: String = {
    var str : String = ""
    minHeap.foreach(str += _ + " ")
    str
  }

}
