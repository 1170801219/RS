import scala.util.Random

val indices = List.range(1, 100).filter(x => x!=6)
List.tabulate(indices.size / 2,3)((x,y)=>if(y==2) 6 else indices.apply(x*2 + y))
