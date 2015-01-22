package geotrellis.spark.sfc

package object zcurve {

  def zdivide(load: (Long, Long, Int, Int) => Long, dims: Int)(xd: Long, rmin: Long, rmax: Long): (Long, Long) = {    
    require(rmin < rmax, "min ($rmin) must be less than max $(rmax)")
    var zmin: Long = rmin
    var zmax: Long = rmax
    var bigmin: Long = 0L
    var litmax: Long = 0L

    def bit(x: Long, idx: Int) = {
      ((x & (1L << idx)) >> idx).toInt
    }
    def over(bits: Long)  = (1L << (bits-1))
    def under(bits: Long) = (1L << (bits-1)) - 1

    var i = 64
    while (i > 0) {
      i -= 1  

      val bits = i/dims+1
      val dim  = i%dims
      
      ( bit(xd, i), bit(zmin, i), bit(zmax, i) ) match {
        case (0, 0, 0) => 
          // continue

        case (0, 0, 1) =>
          zmax   = load(zmax, under(bits), bits, dim)
          bigmin = load(zmin, over(bits), bits, dim)
          // println(s"\t\tBIGMIN=$bigmin")
          // println(s"\t\tMAX=$zmax")
          // return (zmax, bigmin)

        case (0, 1, 0) =>  
          // sys.error(s"Not possible, MIN <= MAX, (0, 1, 0)  at index $i")

        case (0, 1, 1) =>
          bigmin = zmin
          // println(s"\t\tBIGMIN=$bigmin")
          return (litmax, bigmin)

        case (1, 0, 0) =>
          litmax = zmax
          // println(s"\t\tLITMAX=$litmax")
          return (litmax, bigmin)

        case (1, 0, 1) =>          
          litmax = load(zmax, under(bits), bits, dim)
          // println(s"\t\tLITMAX=$litmax")
          zmin = load(zmin, over(bits), bits, dim)
          // println(s"\t\tMIN=$zmin")
          // return (litmax, zmin)

        case (1, 1, 0) =>
//          println(f"XD = ${xd.toBinaryString}%64s")
//          println(f"MN = ${zmin.toBinaryString}%64s")
//          println(f"MX = ${zmax.toBinaryString}%64s")
          // sys.error(s"Not possible, MIN <= MAX, (1, 1, 0) at index $i")
        
        case (1, 1, 1) => 
          // continue          
      }
    }
    (litmax, bigmin)
  }
}