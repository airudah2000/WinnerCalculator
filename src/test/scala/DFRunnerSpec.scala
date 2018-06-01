package com.db.exercise

import java.io.{BufferedReader, File, FileReader}
import java.util.logging.Level

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class DFRunnerSpec extends FunSuite {

  new SparkContext(new SparkConf().setAppName(appName).setMaster(master)).setLogLevel(Level.OFF.toString) //Turn off Excessive logging

  final val appName = "DataFrame-Test"
  final val master = "local[*]"
  final val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()
  final val teamsFilePath = "src/main/resources/teams.dat"
  final val scoresFilePath = "src/main/resources/scores.dat"


  test("Extract"){
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())

    extracted.values.foreach(_.show())

    assert(extracted.headOption.nonEmpty)
    assert(extracted.head._1 === input.result().head._1)
    assert(extracted.head._2.toDF().head().toString() === "[PLAYER1, DAY1, 8.95]")
  }

  test("Transform") {
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())

    val transformed = dfRunner.transform(extracted)
    assert(transformed.count() !== 0)
  }

  test("Load") {
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())
    val transformed = dfRunner.transform(extracted)
    val outputFilePath = "src/test/resources/DataFrameTestResult.dat"
    dfRunner.load(transformed, outputFilePath)

    val outputFile = new File(outputFilePath)
    val br = new BufferedReader(new FileReader(outputFile))
    val lines = br.lines().toArray.toList.map(_.toString)

    assert(lines.head === "TEAM2,20.65")
    assert(lines(1) === "PLAYER1,11.299999999999999")
  }

  test("Empty Files"){
    val emptyTeamsFilePath = "src/test/resources/emptyteams.dat"
    val emptyScoresFilePath = "src/test/resources/emptyscores.dat"

    val dfRunner = new DFRunner(spark)
    val emptyInput = Map.newBuilder[String, String]
    emptyInput.+=("TEAMS" -> emptyTeamsFilePath)
    emptyInput.+=("SCORES" -> emptyScoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(emptyInput.result())

    extracted.foreach(_._2.show()) // Expects empty data frames

    val transformed = dfRunner.transform(extracted)

    val thrown = intercept[Exception]{
      dfRunner.load(transformed, "")
    }

    assert(thrown.getMessage === "DataFrame is empty. Nothing to write")
  }

  test("Multiple Winners"){
    import spark.implicits._

    val playersScoreDataFrame = Seq(
      ("PLAYER1", "DAY1", "8.95"),
      ("PLAYER1", "DAY2", "10.05"),
      // Player 1 total score = 19.0
      ("PLAYER2", "DAY1", "10.00"),
      ("PLAYER2", "DAY2", "9.00"),
      // Player 2 total score = 19.0
      ("PLAYER3", "DAY1", "7.30"),
      ("PLAYER3", "DAY2", "3.70"),
      ("PLAYER4", "DAY3", "3.20"),
      ("PLAYER5", "DAY3", "4.30"),
      ("PLAYER6", "DAY3", "5.40")
    ).toDF("_c0", "_c1", "_c2")

    val playerTeamDataFrame = Seq(
      ("PLAYER1", "TEAM1"),
      ("PLAYER2", "TEAM1"),
      ("PLAYER3", "TEAM1"),
      ("PLAYER4", "TEAM2"),
      ("PLAYER5", "TEAM2"),
      ("PLAYER6", "TEAM3"),
      ("PLAYER7", "TEAM3")
    ).toDF("_c0", "_c1")

    val dfRunner = new DFRunner(spark)

    // Test multiple winning players
    val dataToTransform = Map("TEAMS" -> playerTeamDataFrame, "SCORES" -> playersScoreDataFrame)
    val transformed = dfRunner.transform(dataToTransform)

    assert(transformed.collectAsList().size() === 2) // Expecting two lines of winners, One each for the winning teams and winning players
    assert(transformed.collectAsList().get(0).getAs[Double]("score") === 49) //Winning Team score should be 49

    //Test multiple winning teams
    val playersScoreDataFrame2 = Seq(
      ("PLAYER1", "DAY1", "8.95"),
      ("PLAYER1", "DAY2", "10.05"),
      // Player 1 total score = 19.0
      ("PLAYER2", "DAY1", "10.00"),
      ("PLAYER2", "DAY2", "9.00"),
      // Player 2 total score = 19.0
      ("PLAYER3", "DAY1", "7.30"),
      ("PLAYER3", "DAY2", "3.70"),
      ("PLAYER4", "DAY3", "3.20"),
      ("PLAYER5", "DAY3", "4.30"),
      ("PLAYER6", "DAY3", "49.0")
    ).toDF("_c0", "_c1", "_c2")

    val dataToTransform2 = Map("TEAMS" -> playerTeamDataFrame, "SCORES" -> playersScoreDataFrame2)
    val transformed2 = dfRunner.transform(dataToTransform2)

    assert(transformed2.collectAsList().size() === 2) // Expecting two lines of winners, One each for the winning teams and winning players
    assert(transformed2.collectAsList().get(0).getAs[Double]("score") === transformed2.collectAsList().get(1).getAs[Double]("score")) //Both Winning Player and winning team have the same score

  }

}
