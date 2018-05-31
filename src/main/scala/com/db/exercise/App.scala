package com.db.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  *  We are trying to automate the way we calculate the winners of a very popular online game.<br>
  *  It is a game played in groups where each player belongs to one and only one team.<br>
  *  It's a very popular online game so the number of players can reach many millions and we'll have many many scores to be processed at the end of the game to work out the winners.<br>
  *  <br>
  *  There will be 2 prizes at the end of the game:<br>
  *  - Team winner: the team (or teams) with the maximum number of points scored at the end of the week (aggregating the points scored by each member of that team)<br>
  *  - Individual winner: the player (or players) with more points scored at the end of the week<br>
  *  <br>
  *  The configuration of the teams will be given in a text file like the following:<br>
  *  {{{
  *  PLAYER1, TEAM1
  *  PLAYER2, TEAM1
  *  PLAYER3, TEAM1
  *  PLAYER4, TEAM2
  *  PLAYER5, TEAM2
  *  PLAYER6, TEAM3
  *  PLAYER7, TEAM3
  *  }}}
  *  <br>
  *  Each line contains a (player, team) tuple.<br>
  *  The game duration is a week and the results will be available at the end of it in a text file with following structure:<br>
  *  {{{
  *  PLAYER1, DAY1, 8.95
  *  PLAYER2, DAY1, 10.00
  *  PLAYER3, DAY1, 7.30
  *  PLAYER1, DAY2, 2.35
  *  PLAYER2, DAY2, 9.00
  *  PLAYER3, DAY2, 1.20
  *  PLAYER4, DAY3, 3.20
  *  PLAYER5, DAY3, 4.30
  *  PLAYER6, DAY3, 5.40
  *  }}}
  *  <br>
  *  Each line contains a (player, day, score) tuple where score will be a number between 0 and 10 (2 decimals allowed).<br>
  *  There are input data examples in the src/main/resources folder.<br>
  *  <br>
  *  The expected output will be a file with 2 lines:<br>
  *  first line will contain the name of the winning team and the total score (or comma separated list of names in case that there's more than one)<br>
  *  second line will contain the name of the winning individual player and the total score (or comma separated list of names in case that there's more than one)<br>
  *  <br>
  *  <br>
  *  Implementation details:<br>
  *  <br>
  *  The result has to be achieved in 2 different ways - using Spark RDD and with the help of Spark SQL Framework.<br>
  *  <br>
  *  There are two classes in the [[com.db.exercise]] package that are supposed to implement both approaches:<br>
  *  [[com.db.exercise.RDDRunner]] - contains method stubs to work with RDD<br>
  *  [[com.db.exercise.DFRunner]] - offers the same but using DataFrames<br>
  *  <br>
  *  The main program is a command-line application which expects 4 arguments:<br>
  *  - full path to the team file<br>
  *  - full path to the score file<br>
  *  - full path to the output file produced by [[com.db.exercise.RDDRunner]]<br>
  *  - full path to the output file produced by [[com.db.exercise.DFRunner]]<br>
  *  <br>
  *  <br>
  *  As part of this assignment you will need to:<br>
  *  - Provide method implementations for both [[com.db.exercise.RDDRunner]] and [[com.db.exercise.DFRunner]] classes. When implementing RDD-based methods, feel free to create helper data types and replace scala.AnyRef type parameters with something meaningful.<br>
  *  - Write unit tests<br>
  *  - Modify pom.xml to generate a jar with dependencies (uber-jar)<br>
  *  - <b>Send the zipped project back to us. While sending the code, don't include any binaries/jars/classes. The archive should contain source files only.</b><br>
  *  <br>
  *  You don't need to worry about writing a fully production-ready system, but we want to see clean, well-tested code.
  */
object App {
  def main(args: Array[String]): Unit = {

    val appName = "game-winner-calculator"
    val master = "local[*]"
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()

    val teamFilePath: String = args(0)
    val scoreFilePath: String = args(1)
    val rddOutputPath: String = args(2)
    val dfOutputPath: String = args(3)

    val input: mutable.Builder[(String, String), Map[String, String]] = Map.newBuilder[String, String]

    input.+=("TEAMS" -> teamFilePath)
    input.+=("SCORES" -> scoreFilePath)

    /* Calculate using RDD approach */
    val rddRunner = new RDDRunner(sc)

    val rddExtracted = rddRunner.extract(input.result())
    val rddTransformed = rddRunner.transform(rddExtracted)

    rddRunner.load(rddTransformed, rddOutputPath)

    /* Calculate using DataFrame and SQL approach */
    val dfRunner = new DFRunner(spark)

    val dfExtracted = dfRunner.extract(input.result())
    val dfTransformed = dfRunner.transform(dfExtracted)

    dfRunner.load(dfTransformed, dfOutputPath)
  }
}
