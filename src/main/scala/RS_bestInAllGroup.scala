import driver.KN
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import problems.{SOAnswer, TpMax}
import rngstream.RngStream
import util.Tools

import java.util
import scala.collection.mutable.Map
import scala.util.Random
/**
 * this script implement the RS procedure that add the optimal alternative to all group.
 */

object RS_bestInAllGroup {


  /**
   * do one match in a group
   * @param x
   * @param alpha
   * @param round
   * @param delta
   * @param param
   * @param cov
   * @param n1
   * @return index of best alternative
   */
  def withinGroupMatch(x: List[Int], alpha: Double, round: Double, delta: Double, param: String, cov: String, n1: Int, accum_sim: Accumulator[Long], accum_com_t: Accumulator[Long], accum_sim_t: Accumulator[Long]): (Int,SOAnswer) = {
    val inGroupContainer: java.util.Map[java.lang.Integer,SOAnswer]=new util.HashMap[java.lang.Integer,SOAnswer]()
    for (i <- x) {
      inGroupContainer.put(i, new SOAnswer)
    }
    val kn = new KN(alpha/Math.pow(2D,round.toDouble),delta,param,cov,n1,inGroupContainer)
    kn.runSystem();

    accum_sim += kn.getTotalSample();
    accum_sim_t += kn.getSimTime();
    accum_com_t += kn.getCompareTime();
    (kn.getBestID,kn.getBestAnswer)
  }


  def generateGroupsWithOptimal(optimalSampleIndex: Int, nSys: Int, currentIndices: Array[Int]) = {
    List.tabulate(currentIndices.length / 2,3)((x,y)=>if(y==2) optimalSampleIndex else currentIndices.apply(x*2 + y))
  }

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

    var sysIndices = sc.parallelize(List.range(1,nSys))
    val seedString  = Tools.generateSeedString()


    var round: Double = 1
    while (sysIndices.count()>1) {

      var sysContainer = sysIndices.map(x => (x, {
        val l = new SOAnswer()
        l.setSeedString(seedString)
        l
      }))

      //    each alternative run n1 times simulation
      sysContainer = sysContainer.map((x) => runInitialSimulations(x, param, cov, n1, accum_sim, accum_sim_t))

      val sysPerformance = sysContainer.map(x => (x._1, x._2.getFn))

      //    choose alternative with best sample performance
      val sampleOptimal = sysPerformance.reduce((x, y) => if (x._2 > y._2) x else y)
      val currentIndices = sysIndices.collect()

      //    generate match groups
      val groups = sc.parallelize(generateGroupsWithOptimal(sampleOptimal._1, nSys,currentIndices))
      val match_result = groups.map(x => withinGroupMatch(x, alpha, round, delta, param, cov, n1, accum_sim, accum_com_t, accum_sim_t))
//      sysContainer.foreach(println)
//      println(s"sample optimal index ${sampleOptimal}")

      sysIndices = match_result.map(x => x._1).union(sc.parallelize(List{sampleOptimal._1})).distinct()
//      println(s"survivalSys after round ${round}")
//      sysIndices.foreach(println)
      round += 1.0
    }
    val final_t = (System.nanoTime() - start_t).toDouble/1e9;
    println(f"best alternative ${sysIndices.reduce((x,y)=>x)}")
    println(f"Total time = $final_t%.2f secs.");
    println(f"total simulation count ${accum_sim.value} times");
    println(f"total simulation time ${accum_sim_t.value.toDouble/1e9} s");
    println(f"total comparison time ${accum_com_t.value.toDouble/1e9} s");
    sc.stop
  }

  /**
   * run initial fix number simulations for an alternative
   * @param x parallelize information of one system
   * @param param
   * @param cov
   * @param n1
   * @param accum_sim
   * @param accum_sim_t
   * @return index of system, performance result of class problems.SOAnswer
   */
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
