package com.db.exercise

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class DFRunnerSpec extends FunSuite {

  Logger.getLogger("com.db.exercise").setLevel(Level.OFF)

  final val appName = "DataFrame-Test"
  final val master = "local[*]"
  final val conf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
  final val sc = new SparkContext(conf)
  final val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()
  final val teamsFilePath = "src/main/resources/teams.dat"
  final val scoresFilePath = "src/main/resources/scores.dat"

  ignore("Extract"){
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("Test Extract" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())

    extracted.values.foreach(_.show())

    assert(extracted.headOption.nonEmpty)
    assert(extracted.head._1 === input.result().head._1)
    assert(extracted.head._2.toDF().head().toString() === "[PLAYER1, DAY1, 8.95]")

  }

  ignore("Transform") {
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())

    val transformed = dfRunner.transform(extracted)

    assert(transformed.count() !== 0)

    transformed.show()
  }

  test("Load") {
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())
    val transformed = dfRunner.transform(extracted)
    val outputFilePath = "src/test/resources/result.dat"
    dfRunner.load(transformed, outputFilePath)

    val output = Map.newBuilder[String, String]
    output.+=("RESULT" -> outputFilePath)
    val outputResult = dfRunner.extract(output.result())

    assert(outputResult.values.head.toString() == "GGG")

  }

}
