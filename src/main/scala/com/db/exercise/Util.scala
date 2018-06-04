package com.db.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait Util {
  //Just some constants.. to avoid typos
  final val TEAMS  = "TEAMS"
  final val SCORES = "SCORES"

  final val winner = "winner"
  final val score  = "score"
  final val day    = "day"
  final val player = "player"
  final val team   = "team"

  def newRdd(items: Seq[Any])(implicit sc: SparkContext): RDD[Any] = sc.parallelize(items)

}

abstract class Game extends Serializable
case class Team(player: String, team: String) extends Game {
  def toRdd (implicit sc: SparkContext): RDD[Team] = sc.parallelize(Seq(this))
}
case class Scores(playerScore: SomeScore, day: String) extends Game {
  def toRdd (implicit sc: SparkContext): RDD[Scores] = sc.parallelize(Seq(this))
}
case class SomeScore(player: String, score: Double) extends Serializable
case class Winner(winner: String, score: Double) extends Serializable

case class ScoreBoard(player: String, score: Double, team: String) extends Serializable

