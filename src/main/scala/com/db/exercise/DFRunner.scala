package com.db.exercise

import org.apache.spark.sql._
import scala.collection.mutable


class DFRunner(val spark: SparkSession) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = for (path <- input) yield {
    val key: String = path._1
    val df: DataFrame = spark.read.csv(path._2).toDF()
    key -> df
  }

  /**
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, DataFrame]): DataFrame = {
    import org.apache.spark.sql
    import org.apache.spark.sql.functions._
    import scala.collection.JavaConverters._

    val playerTeamMap: Map[String, DataFrame] = extracted
      .filter(_._1 == "TEAMS")
      .map({ case (k, v) => k -> v.toDF("player", "team") })

    val playerScoresMap: Map[String, DataFrame] = extracted
      .filter(_._1 == "SCORES")
      .map({ case (k, v) =>
        k -> v.withColumn("_c2", v("_c2").cast(sql.types.DoubleType)).toDF("player", "day", "score")
      })

    val summedUp: Option[Dataset[Row]] = playerScoresMap.values.map(x => x.groupBy("player")
      .sum("score")
      .sort(desc("sum(score)")))
      .headOption


    val maximumScore: Double = playerScoresMap.map(x => x._2.groupBy("player")
      .sum("score")
      .sort(desc("sum(score)"))
      .select("sum(score)")
      .collectAsList()
      .asScala
      .map(y => y.getAs[Double]("sum(score)")))
      .headOption match {
      case Some(xx) => xx.max
      case None => 0
    }

    println(s"maximumScore = ${maximumScore}")
    summedUp match {
      case Some(row: Dataset[Row]) => row.show()
      case None =>
    }


    val playersWithMaxScore: DataFrame = summedUp match {
      case Some(row: Dataset[Row]) => row.toDF()
      case None => spark.emptyDataFrame
    }

    playersWithMaxScore.show()

    ???
  }

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = ???
}
