import driver.KN
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import problems.{SOAnswer, TpMax}
import rngstream.RngStream
import util.Tools

import java.io.{File, PrintWriter}
import java.util

/**
 * this script implement the RS procedure that add the optimal alternative to all group.
 */

object RS_bestVersusAllOther {


  def generateGroupsWithOptimal(optimalSampleIndex: Int, nSys: Int, currentIndices: Array[Int]) = {
    List.tabulate(currentIndices.length - 1, 2)((x, y) => if (y == 1) optimalSampleIndex else currentIndices.apply(x))
  }

  def main(args: Array[String]): Unit = {

    //    parse command line argument
    val n1 = args(0).toInt;
    val alpha = args(1).toDouble;
    val delta = args(2).toDouble;
    val param = args(3);
    val cov = args(4);
    val nCores = args(5).toInt;
    val repeats = args(6).toInt;

    for (i <- 1 to repeats) {
      val writer = new PrintWriter(new File(f"/home/jin/Desktop/RS/record_bestVersusAll/param ${param}/${i}.txt"))

      println(s"iteration ${i}")
      RS(n1, alpha, delta, param, cov, nCores, writer)
      writer.close()
    }


  }

  def RS(n1: Int, alpha: Double, delta: Double, param: String, cov: String, nCores: Int, writer: PrintWriter) = {
    val start_t = System.nanoTime()

    //     we assign tasks by spark scheduler instead of assigning task to specific processor here
    val conf = new SparkConf().setAppName("KT-bestInAll-Spark").set("spark.cores.max", nCores.toString());
    val sc = new SparkContext(conf);

    //    prepare for ranking $ selection
    val nSys = TpMax.getNumSystems(param).toInt;


    //    accumulate simulation information
    val accum_sim = sc.accumulator(0L, "Accumulator: total sample size");
    val accum_sim_t = sc.accumulator(0L, "Accumulator: total simulation time");
    val accum_com_t = sc.accumulator(0L, "Accumulator: comparison time");

    println(s"Finish simulation initialization, system count(${nSys})")
    println(s"Perform initial simulation to find sample best one, n1(${n1})")

    var sysIndices = sc.parallelize(List.range(1, nSys))
    val seedString = Tools.generateSeedString()


    var round: Double = 1
    var count = sysIndices.count()

    while (count > 1) {
      //      println(f"round ${round} start, rest alternatives:${count}")
      //      writer.println(f"round ${round} , rest alternatives:${count}")

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

      //    generate match groups
      // use single machine generate groups
      //      val currentIndices = sysIndices.collect()
      //      val groups = sc.parallelize(generateGroupsWithOptimal(sampleOptimal._1, nSys,currentIndices))
      // use RDD generate groups
      val groups = sysIndices.filter(x => x != sampleOptimal._1).map(x => List(x, sampleOptimal._1))
      val match_result = groups.map(x => withinGroupMatch(x, alpha, round, delta, param, cov, n1, accum_sim, accum_com_t, accum_sim_t))
      //      sysContainer.foreach(println)
      //      println(s"sample optimal index ${sampleOptimal}")
      if (count <= 2) {
        sysIndices = match_result.map(x => x._1)
      } else {
        sysIndices = match_result.map(x => x._1).union(sc.parallelize(List {
          sampleOptimal._1
        })).distinct()
      }
      //      println(s"survivalSys after round ${round}")
      //      sysIndices.foreach(println)
      round += 1.0
      count = sysIndices.count()
      if (false) {
        println(f"sample optimal: ${sampleOptimal}")
        println("compare groups:")
        groups.foreach(println)
        println("survival sysIndices:")
        sysIndices.foreach(println)
        println("match_result:")
        match_result.foreach(println)
      }
    }
    val final_t = (System.nanoTime() - start_t).toDouble / 1e9;
    println(f"best alternative ${sysIndices.reduce((x, y) => x)}")
    println(f"Total time = $final_t%.2f secs.");
    println(f"total simulation count ${accum_sim.value} times");
    println(f"total simulation time ${accum_sim_t.value.toDouble / 1e9} s");
    println(f"total comparison time ${accum_com_t.value.toDouble / 1e9} s");

    //    writer.println(f"param ${param}")
    writer.println(f"best alternative ${sysIndices.reduce((x, y) => x)}")
    writer.println(f"Total time = $final_t%.2f secs.");
    writer.println(f"total simulation count ${accum_sim.value} times");
    writer.println(f"total simulation time ${accum_sim_t.value.toDouble / 1e9} s");
    writer.println(f"total comparison time ${accum_com_t.value.toDouble / 1e9} s");
    sc.stop
  }

  /**
   * do one match in a group
   *
   * @param x
   * @param alpha
   * @param round
   * @param delta
   * @param param
   * @param cov
   * @param n1
   * @return index of best alternative
   */
  def withinGroupMatch(x: List[Int], alpha: Double, round: Double, delta: Double, param: String, cov: String, n1: Int, accum_sim: Accumulator[Long], accum_com_t: Accumulator[Long], accum_sim_t: Accumulator[Long]): (Int, SOAnswer) = {
    val inGroupContainer: java.util.Map[java.lang.Integer, SOAnswer] = new util.HashMap[java.lang.Integer, SOAnswer]()
    for (i <- x) {
      inGroupContainer.put(i, new SOAnswer)
    }
    val kn = new KN(alpha / Math.pow(2D, round.toDouble), delta, param, cov, n1, inGroupContainer)
    kn.runSystem();

    accum_sim += kn.getTotalSample();
    accum_sim_t += kn.getSimTime();
    accum_com_t += kn.getCompareTime();
    (kn.getBestID, kn.getBestAnswer)
  }

  /**
   * run initial fix number simulations for an alternative
   *
   * @param x parallelize information of one system
   * @param param
   * @param cov
   * @param n1
   * @param accum_sim
   * @param accum_sim_t
   * @return index of system, performance result of class problems.SOAnswer
   */
  def runInitialSimulations(x: (Int, SOAnswer), param: String, cov: String, n1: Int, accum_sim: Accumulator[Long], accum_sim_t: Accumulator[Long]): (Int, SOAnswer) = {
    val id = x._1
    val answer = x._2

    val prob = new TpMax(param, cov);
    val rStream = new RngStream();
    rStream.setSeed(RngStream.StrToSeed(answer.getSeedString));


    val _t = System.currentTimeMillis();
    prob.runSystem(id, n1, rStream)

    accum_sim_t += System.currentTimeMillis() - _t;

    (id, prob.getAns)
  }

}
