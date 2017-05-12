package mr.hw4.scala.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Driver program executes the steps required to calculate the page rank.
 * Detailed description of each function is described in the report.
 */
object Driver {

  def main(args: Array[String]): Unit =
    {
      // Initialize local variables
      var inputPath = "input"
      var outputPath = "output"
      var alpha = 0.15
      var noOfIterations = 10
      var kForTopK = 100
      
      val t1 = System.nanoTime()
      println("PageRank program started.");
      
      // Update local variables from args array
      if (args.length == 5) {
        println("Input Arguments :: " + args(0) + " " + args(1) + " " + args(2) + " " + args(3) + " " + args(4))
        inputPath = args(0)
        outputPath = args(1)
        alpha = args(2).toDouble
        noOfIterations = args(3).toInt
        kForTopK = args(4).toInt
      }
      
      // create spark context
      val conf = new SparkConf().setAppName("PageRank")
      val sc = new SparkContext(conf)
      
      // read input data from inputPath location
      val tf = sc.textFile(inputPath)

      // generate the graph structure from the input data and persist it
      val nodes = tf
        .flatMap(HelperFunctions.flattenAdjacencyList)
        .reduceByKey(HelperFunctions.reduceNodesByNodeId)
        .persist()

      val noOfNodes = nodes.count()
      val t2 = System.nanoTime()
      var deltaStr = ""

      // Initialize the page rank of each node
      var ranks = nodes.mapValues(value => 1.0 / noOfNodes)
      
      // page rank iterations
      for (i <- 1 to noOfIterations) {

        // calculate the page rank contribution to each node along with delta
        val contribs = nodes
          .join(ranks)
          .flatMap(HelperFunctions.flattenRanks)
          .reduceByKey(_ + _)
          
        // lookup delta 
        val delta = contribs.lookup("DANGLING~FACTOR")
        deltaStr += "Delta after iteration " + i + " = " + delta(0) + "\n"
        
        // update the page rank of each node by applying delta and alpha
        ranks = contribs.mapValues({ rank =>
          (alpha / noOfNodes) + (1 - alpha) * ((delta(0) / noOfNodes) + rank)
        })
      }
      
      // enable the below line to write the page ranks of all nodes to file
      //ranks.saveAsTextFile(outputPath + "/AllRanks")
      
      // find the top k nodes by page rank
      sc.parallelize(ranks
        .filter(node => !node._1.equals("DANGLING~FACTOR"))
        .map(node => (node._2, node._1))
        .top(kForTopK).toSeq, 1)
        .map(node => (node._2, node._1))
        .saveAsTextFile(outputPath + "/TopKRanks")

      val t3 = System.nanoTime()
      
      // print the statistics
      println("PageRank program successfully finished.");
      println("No of Nodes = " + noOfNodes)
      println("Preprocessing Running Time (s) = " + (t2 - t1)/1000000000)
      println("Page Rank Calculations and TopK Running Time (s) = " + (t3 - t2)/1000000000)
      println("Total Running Time(s) = " + (t3 - t1)/1000000000)
      println("Delta after each iteration = ")
      println(deltaStr)
      sc.stop()
    }
}