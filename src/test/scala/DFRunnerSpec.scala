package com.db.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class DFRunnerSpec extends FunSuite {

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

  test("Transform") {
    val dfRunner = new DFRunner(spark)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, DataFrame] = dfRunner.extract(input.result())

    val transformed = dfRunner.transform(extracted)

    assert(transformed.count() !== 0)
  }

}
