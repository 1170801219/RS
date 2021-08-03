import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import problems.{SOAnswer, TpMax}
import rngstream.RngStream
import util.Tools

import scala.util.Random
/**
 * this script implement the RS procedure that add the optimal alternative to all group.
 */

object RS_bestInAllGroup {
  def main(args: Array[String]): Unit = {
    val start_t = System.nanoTime()

//    parse command line argument
    val n1 = args(0).toInt;
    val alpha = args(1).toDouble;
    val delta = args(2).toDouble;
    val param = args(3);
    val cov = args(4);
    val nCores = args(5).toInt;

//     we assign tasks by spark scheduler instead of assigning task to specific processor here
    val conf = new SparkConf().setAppName("KT-bestInAll-Spark").set("spark.cores.max",nCores.toString());
    val sc = new SparkContext(conf);

//    prepare for ranking $ selection
    val nSys=TpMax.getNumSystems(param).toInt;


//    accumulate simulation information
    val accum_sim = sc.accumulator(0L,"Accumulator: total sample size");
    val accum_sim_t = sc.accumulator(0L,"Accumulator: total simulation time");
    val accum_com_t = sc.accumulator(0L, "Accumulator: comparison time");

    println(s"Finish simulation initialization, system count(${nSys})")
    println(s"Perform initial simulation to find sample best one, n1(${n1})")

    val sysIndices = sc.parallelize(List.range(1,nSys))
    val seedString  = Tools.generateSeedString()
    var sysContainer = sysIndices.map(x=> (x, {
      val l =new SOAnswer()
      l.setSeedString(seedString)
      l
    }))

//    each alternative run n1 times simulation
    sysContainer = sysContainer.map((x)=>runInitialSimulations(x,param,cov,n1,accum_sim,accum_sim_t) )

    val sysPerformance = sysContainer.map(x => (x._1,x._2.getFn))

//    choose alternative with best sample performance
    val sampleOptimal = sysPerformance.reduce((x,y) => if (x._2 > y._2) x else y)

    sysContainer.foreach(println)
    println(sampleOptimal)

  }

  def runInitialSimulations(x: (Int, SOAnswer), param: String, cov: String,n1:Int, accum_sim: Accumulator[Long], accum_sim_t: Accumulator[Long]): (Int,SOAnswer) ={
    val id = x._1
    val answer = x._2

    val prob = new TpMax(param,cov);
    val rStream = new RngStream();
    rStream.setSeed(RngStream.StrToSeed(answer.getSeedString));


    val _t = System.currentTimeMillis();
    prob.runSystem(id,n1,rStream)

    accum_sim_t+=System.currentTimeMillis()-_t;

     (id,prob.getAns)
  }

}
