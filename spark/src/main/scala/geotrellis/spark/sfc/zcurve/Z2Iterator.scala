package geotrellis.spark.sfc.zcurve

class Z2Iterator(min: Z2, max: Z2) extends Iterator[Z2] {
  private var cur = min
  
  def hasNext: Boolean = cur.z <= max.z
  
  def next: Z2 = {
    val ret = cur
    cur += 1
    ret
  }

  def seek(min: Z2, max: Z2) = {
    cur = min
  }
}

case class ZdivideIterator(min: Z2, max: Z2) extends Z2Iterator(min, max)  {
  val MAX_MISSES = 10
  val range = Z2Range(min, max)
  var haveNext = false
  var _next: Z2 = new Z2(0)
  
  advance
  
  override def hasNext: Boolean = haveNext
  
  override def next: Z2 = {
    // it's safe to report cur, because we've advanced to it and hasNext has been called.
    val ret = _next
    advance
    ret
  }

  /**
   * Two possible post-conditions:
   * 1. cur is set to a valid object
   * 2. cur is set to null and source.hasNext == false  
   */
  def advance: Unit = {
    var misses = 0
    while (misses < MAX_MISSES && super.hasNext) {
      _next = super.next
      if (range.contains(_next)) {
        haveNext = true
        return
      } else {
        misses + 1
      }
    }
    
    if (_next < max) {
      val (litmax, bigmin) = Z2.zdivide(_next, min, max)
      _next = bigmin
      haveNext = true
    } else {
      haveNext = false
    }
  }
}