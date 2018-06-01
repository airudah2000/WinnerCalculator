package com.db.exercise

import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


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

    // Get two collections. One for each file
    val playerTeamMap: Map[String, DataFrame] = extracted.filter(_._1 == "TEAMS")
      .map({ case (k, v) => k -> v.toDF("player", "team") })

    val playerScoresMap: Map[String, DataFrame] = extracted.filter(_._1 == "SCORES")
      .map({ case (k, v) =>
        k -> v.withColumn("_c2", v("_c2").cast(sql.types.DoubleType)).toDF("player", "day", "score")
      })

    // Sum up each players' score and sort by players score
    val playersTotalScores: Option[Dataset[Row]] = playerScoresMap.values.map(_.groupBy("player")
      .sum("score")
      .sort(desc("sum(score)")))
      .headOption

    // Find the highest player score
    val maximumPlayerScore: Double = playersTotalScores match {
      case Some(ds: Dataset[Row]) =>
        ds.collectAsList()
          .asScala
          .map(y => y.getAs[Double]("sum(score)"))
          .max
      case None => 0
    }

    // Find all players with the Maximum score
    val winningPlayers: DataFrame = playersTotalScores match {
      case Some(row: Dataset[Row]) => row.filter(r => r.getAs[Double]("sum(score)") == maximumPlayerScore)
      case None => spark.emptyDataFrame
    }

    //Sum up team scores and sort by scores
    def winningTeams: DataFrame = {
      val totalScoresDF: DataFrame = playersTotalScores.get.toDF() //OrElse(spark.emptyDataset).toDF()
      val playerTeamDF: DataFrame = playerTeamMap.values.head.toDF() //OrElse(spark.emptyDataset).toDF()

      val totalTeamScore = totalScoresDF.join(playerTeamDF, Seq("player"))
        .groupBy("team")
        .sum("sum(score)")
        .sort(desc("sum(sum(score))"))

      val winningTeamScore: Double = totalTeamScore.collectAsList()
        .asScala
        .map(r => r.getAs[Double]("sum(sum(score))"))
        .max

      totalTeamScore.filter(row => row.getAs[Double]("sum(sum(score))")
        .equals(winningTeamScore))
        .toDF("winner", "score")
    }

    winningPlayers union winningTeams toDF("winner", "score")

  }

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = {
    transformed.write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "false")
//      .option("delimiter", ",")
      .save(path)
  }
}
