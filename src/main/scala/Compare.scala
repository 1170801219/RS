import RS_bestInAllGroup.withinGroupMatch
import org.apache.spark.{SparkConf, SparkContext}
import problems.TpMax

import java.io.{File, PrintWriter}

object Compare {

  def main(args: Array[String]): Unit = {
    val n1 = args(0).toInt;
    val alpha = args(1).toDouble;
    val delta = args(2).toDouble;
    val param = args(3);
    val cov = args(4);
    val nCores = args(5).toInt;
    val nSysInGroup = args(6).toInt;
    val writer = new PrintWriter(new File(f"large size compare.txt"))

    for (i <- List.range(30, 40, 2)) {
      println(f"\n\n\ntotal system number ${TpMax.getNumSystems(i.toString)}")
      writer.println(f"\n\n\ntotal system number ${TpMax.getNumSystems(i.toString)}")
      writer.println(f"KT+")
      println(f"KT+")
      demo.RS(n1, alpha, delta, i.toString, cov, nCores, nSysInGroup, writer)
      writer.println(f"\nbest in all group")
      println(f"\nbest in all group")
      RS_bestInAllGroup.RS(n1, alpha, delta, i.toString, cov, nCores, writer)
      writer.println(f"\nbest versus all group")
      println(f"\nbest versus all group")
      RS_bestVersusAllOther.RS(n1, alpha, delta, i.toString, cov, nCores, writer)
      writer.println(f"\nKN")
      println(f"\nKN")
      KN(n1, alpha, delta, i.toString, cov, nCores, writer)
      writer.flush()
    }
    writer.close()

  }

  def KN(n1: Int, alpha: Double, delta: Double, param: String, cov: String, nCores: Int, writer: PrintWriter) = {
    val start_t = System.nanoTime()
    val conf = new SparkConf().setAppName("KN-Spark").set("spark.cores.max", nCores.toString());
    val sc = new SparkContext(conf);
    val accum_sim = sc.accumulator(0L, "Accumulator: total sample size");
    val accum_sim_t = sc.accumulator(0L, "Accumulator: total simulation time");
    val accum_com_t = sc.accumulator(0L, "Accumulator: comparison time");
    val result = withinGroupMatch(List.range(1, TpMax.getNumSystems(param).toInt), alpha, 1, delta, param, cov, n1, accum_sim, accum_com_t, accum_sim_t)

    val final_t = (System.nanoTime() - start_t).toDouble / 1e9;
    sc.stop()
    println(f"best alternative ${result._1}")
    println(f"Total time = $final_t%.2f secs.");
    println(f"total simulation count ${accum_sim.value} times");
    println(f"total simulation time ${accum_sim_t.value.toDouble / 1e9} s");
    println(f"total comparison time ${accum_com_t.value.toDouble / 1e9} s");

    writer.println(f"param ${param}")
    writer.println(f"best alternative ${result._1}")
    writer.println(f"Total time = $final_t%.2f secs.");
    writer.println(f"total simulation count ${accum_sim.value} times");
    writer.println(f"total simulation time ${accum_sim_t.value.toDouble / 1e9} s");
    writer.println(f"total comparison time ${accum_com_t.value.toDouble / 1e9} s");

  }

}
