package com.db.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDRunner(val context: SparkContext) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[RDD]] per each input
    */
  def extract(input: Map[String, String]): Map[String, RDD[AnyRef]] = ???

  /**
    * @param extracted a map of [[RDD]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, RDD[AnyRef]]): RDD[AnyRef] = ???

  /**
    * @param transformed the [[RDD]] to store as a file
    * @param path the path to save the output file
    */
  def load(transformed: RDD[AnyRef], path: String): Unit = ???

}
