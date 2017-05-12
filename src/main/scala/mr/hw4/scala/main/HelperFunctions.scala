package mr.hw4.scala.main

import scala.collection.mutable.ListBuffer

import mr.hw4.java.utility.Bz2WikiParser
import mr.hw4.java.utility.MyParser

/**
 * This clss provides the static methods to be used by the Driver program
 */
object HelperFunctions {

  /**
   * This function parses each line and returns the nodeId and adjacencyList
   * as an array of PairRDD
   */
  def flattenAdjacencyList(s: String): Array[(String, Array[String])] =
    {
      // parse the line
      val node = MyParser.parse(s)
      
      val listBuf = new ListBuffer[(String, Array[String])]

      if (null != node) {
        val adjacencyList = node.getAdjacencyList()

        if (null != adjacencyList && adjacencyList.length > 0) {
          
          // add this nodeId with its adjacencyList in array
          listBuf += ((node.getNodeId(), adjacencyList))

          for (i <- 0 until adjacencyList.length) {
            
            // add all adjacent nodes in the array with null as adjacency list to handle dangling nodes
            listBuf += ((adjacencyList(i), null));
          }
        } else {
          
          // dangling node
          listBuf += ((node.getNodeId(), null))
        }
      }
      return listBuf.toArray
    }

  /**
   * This function reduces two values to one, it returns the valu whose
   * adjacencyList is not null else null
   */
  def reduceNodesByNodeId(val1: Array[String], val2: Array[String]): Array[String] =
    {
      if (null != val1) {
        return val1
      }
      if (null != val2) {
        return val2
      }
      return null
    }

  /**
   * This function returns the array of PairRDD of nodeId and page rank contribution
   * to that node. It handles the dangling nodes by passing delta as a dummy key.
   */
  def flattenRanks(node: (String, (Array[String], Double))): Array[(String, Double)] =
    {
      val listBuf = new ListBuffer[(String, Double)]

      val nodeId = node._1
      val adjacencyList = node._2._1
      val rank = node._2._2

      listBuf += ((nodeId, 0.0))

      if (null != adjacencyList && adjacencyList.length > 0) {
        for (i <- 0 until adjacencyList.length) {
          listBuf += ((adjacencyList(i), rank / adjacencyList.length));
        }
      } else {
        listBuf += (("DANGLING~FACTOR", rank))
      }
      return listBuf.toArray
    }
}