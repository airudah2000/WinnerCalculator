package com.db.exercise

import java.io.{File, FileNotFoundException}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDRunner(val context: SparkContext) extends Util {

  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[RDD]] per each input
    */
  def extract(input: Map[String, String]): Map[String, RDD[Game]] = {

    input.par.map({ case (key, value) =>
      key match {
        case TEAMS => {
          val teams: RDD[Game] = context.textFile(value)
            .map(x => x.split(","))
            .map({ case Array(x: String, y: String) => Team(x, y.replace(" ", ""))} )
          key -> teams
        }
        case SCORES => {
          val scores: RDD[Game] = context.textFile(value)
            .map(x => x.stripMargin.split(","))
            .map({ case Array(x: String, y: String, z: String) => Scores(SomeScore(x, z.toDouble), y) })
          key -> scores
        }
      }

    }).seq

  }

  /**
    * @param extracted a map of [[RDD]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, RDD[Game]]): RDD[Winner] = {

    val playerScoresRdd: RDD[_ >: Scores <: Game] = extracted.getOrElse(SCORES, context.parallelize(Seq.empty[Scores]))
    val teamsRdd: RDD[_ >: Team <: Game] = extracted.getOrElse(TEAMS, context.parallelize(Seq.empty[Team]))

    val playerScoreBoard: RDD[SomeScore] = playerScoresRdd.map(x => x.asInstanceOf[Scores])
      .groupBy(y => y.playerScore.player)
      .map(z => SomeScore(z._1, z._2.toList.map(_.playerScore.score).sum))
      .sortBy(_.score)

    if (playerScoreBoard.isEmpty()) {
      context.parallelize(Seq.empty[Winner])
    } else {

      val winningPlayer: Winner = playerScoreBoard.groupBy(psb => psb.score)
        .map({ case (s, p) => Winner(p.map(_.player).mkString(","), s) })
        .sortBy(_.score, ascending = false) //Top score bubbles to the top
        .first()

      def findTeam(player: String): Team = teamsRdd.collect.map(_.asInstanceOf[Team]).filter(t => t.player == player).head

      //FIXME: There's a flaw in the (design?) and use of Game abstract class (i.e. RDD[_ >: Team <: Game])
      //FIXME: which means raises a need to cast `teamsRdd` to type [Team] first before calling .find/.filter.
      //FIXME: Unfortunately this is not allowed with RDDs, so have resorted to working around this with .collect
      //FIXME: and .par which isn't ideal
      val teamScoreBoard: Seq[ScoreBoard] = (for (player <- playerScoreBoard.collect.par) yield {
        val theTeam: Team = findTeam(player.player)
        ScoreBoard(player.player, player.score, theTeam.team)
      }).seq


      val winningTeam: Winner = context.parallelize(teamScoreBoard).groupBy(tsb => tsb.team)
        .map({ case (key, value) => key -> value.map(_.score).sum })
        .groupBy(s => s._2)
        .map({ case (score, winnerScore) => Winner(winnerScore.map(_._1).mkString(","), score) })
        .sortBy(_.score, ascending = false) //Top score bubbles to the top
        .first()

      context.makeRDD(Seq(winningTeam, winningPlayer))
    }
  }

  /**
    * @param transformed the [[RDD]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: RDD[Winner], path: String): Unit = {

    val tmpDir = path + ".tmp"

    // FIXME: coalesce(1) Not efficient for large datasets
    transformed.coalesce(1, false)
      .sortBy(_.winner.substring(0,1), ascending = false).map { w => w.winner.stripMargin + "," + w.score.toString
    }.saveAsTextFile(tmpDir)

    val dir = new File(tmpDir)
    val outputFile: Option[File] = dir.listFiles.find(f => f.getName.startsWith("part-0000"))
    outputFile match {
      case Some(f) => f.renameTo(new File(path))
      case None => throw new FileNotFoundException()
    }

    dir.listFiles.foreach(f => f.delete)
    dir.deleteOnExit()

  }

}
