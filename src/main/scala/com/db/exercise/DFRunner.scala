package com.db.exercise

import java.io.{File, FileNotFoundException}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.collection.JavaConverters._

class DFRunner(val spark: SparkSession) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = for (path <- input) yield {
    val key: String = path._1
    val df: DataFrame = {
      val isEmpty = spark.read.textFile(path._2).collectAsList().isEmpty
      if (isEmpty) spark.emptyDataFrame else spark.read.csv(path._2)
    }
    key -> df
  }

  /**
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, DataFrame]): DataFrame = {

    // Split into two collections. One for each expected file
    val playerTeamMap: Map[String, DataFrame] = extracted.filter(_._1 == "TEAMS")
    val playerScoresMap: Map[String, DataFrame] = extracted.filter(_._1 == "SCORES")

    // TODO: checks that we have value in both files. Needs to be simplified
    if (playerTeamMap.values.head.collectAsList().isEmpty|| playerScoresMap.values.head.collectAsList().isEmpty) {
      spark.emptyDataFrame
    } else {
      // Name the columns
      val playerScores = playerScoresMap.values.map(s => s.withColumn("_c2", s("_c2").cast(DoubleType)).toDF("player", "day", "score"))
      val playerTeam = playerTeamMap.values.map(pt => pt.toDF("player", "team"))

      // Sum up each players' score and sort by players score
      val playersTotalScores: Option[Dataset[Row]] = playerScores.map(_.groupBy("player")
        .sum("score")
        .sort(desc("sum(score)")))
        .headOption

      playersTotalScores.get.show()

      // Find the maximum player score
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

      winningPlayers.show()

      //Sum up each team's score and sort by scores
      def winningTeams: DataFrame = {
        val totalScoresDF: DataFrame = playersTotalScores.getOrElse(spark.emptyDataFrame).toDF()
        val playerTeamDF: DataFrame = playerTeam.headOption.getOrElse(spark.emptyDataFrame).toDF()

        val totalTeamScore = totalScoresDF.join(playerTeamDF, Seq("player"))
          .groupBy("team")
          .sum("sum(score)")
          .sort(desc("sum(sum(score))"))

        val winningTeamScore: Double = totalTeamScore.collectAsList()
          .asScala
          .map(r => r.getAs[Double]("sum(sum(score))"))
          .max

        val winningTeams = totalTeamScore.filter(row => row.getAs[Double]("sum(sum(score))")
          .equals(winningTeamScore))
          .toDF("winner", "score")

        winningTeams.show()
        winningTeams
      }

      winningPlayers union winningTeams toDF("winner", "score")

    }
  }

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = {

    if(transformed.collectAsList().isEmpty) throw new Exception("DataFrame is empty. Nothing to write")
    else {
      val tmpDir = path + ".tmp"

      transformed
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter", ",")
        .csv(tmpDir)

      val dir = new File(tmpDir)
      val outputFile: Option[File] = dir.listFiles.find(f => f.getName.startsWith("part-00000"))
      outputFile match {
        case Some(f) => f.renameTo(new File(path))
        case None => throw new FileNotFoundException()
      }
      dir.listFiles.foreach(f => f.delete)
      dir.deleteOnExit()
    }
  }
}
