import java.io.{BufferedReader, File, FileReader}

import com.db.exercise._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class RDDRunnerSpec extends FunSuite with Util with TestUtil {

  final val teamsFilePath = "src/main/resources/teams.dat"
  final val scoresFilePath = "src/main/resources/scores.dat"

  test("Extract"){
    val rddRunner = new RDDRunner(sc)
    val input = Map.newBuilder[String, String]
    input.+=(SCORES -> scoresFilePath)

    val extracted: Map[String, RDD[Game]] = rddRunner.extract(input.result())

    val linesInFile = extracted.values.map(x => x.collect().length).sum
    assert(linesInFile === 5)
  }

  test("Transform") {
    val rddRunner = new RDDRunner(sc)
    val input = Map.newBuilder[String, String]
    input.+=(TEAMS-> teamsFilePath)
    input.+=(SCORES -> scoresFilePath)
    val extracted: Map[String, RDD[Game]] = rddRunner.extract(input.result())
    val transformed: RDD[Winner] = rddRunner.transform(extracted)

    transformed.collect().toList.foreach(x => println(s"${x.winner} : ${x.score}"))

    assert(transformed.collect().length == 2)
  }

  test("Load") {
    val rddRunner = new RDDRunner(sc)
    val input = Map.newBuilder[String, String]
    input.+=("TEAMS" -> teamsFilePath)
    input.+=("SCORES" -> scoresFilePath)
    val extracted: Map[String, RDD[Game]] = rddRunner.extract(input.result())
    val transformed = rddRunner.transform(extracted)
    val outputFilePath = "src/test/resources/RddTestResult.dat"
    rddRunner.load(transformed, outputFilePath)

    val outputFile = new File(outputFilePath)
    val br = new BufferedReader(new FileReader(outputFile))
    val lines = br.lines().toArray.toList.map(_.toString)

    assert(lines.head === "TEAM2,20.65")
    assert(lines(1) === "PLAYER1,11.299999999999999")
  }

  test("Empty Files"){
    val emptyTeamsFilePath = "src/test/resources/emptyteams.dat"
    val emptyScoresFilePath = "src/test/resources/emptyscores.dat"

    val rddRunner = new RDDRunner(sc)
    val emptyInput = Map.newBuilder[String, String]
    emptyInput.+=("TEAMS" -> emptyTeamsFilePath)
    emptyInput.+=("SCORES" -> emptyScoresFilePath)
    val extracted: Map[String, RDD[Game]] = rddRunner.extract(emptyInput.result())
    assert(extracted.map(x => x._2.collect().length).sum === 0)

    val transformed = rddRunner.transform(extracted)
    assert(transformed.collect().length === 0)
  }

  test("Multiple Winners"){
    val playersScoreRdd: RDD[Game] = sc.parallelize(Seq(
      Scores(SomeScore("PLAYER1", 8.95), "DAY1"),
      Scores(SomeScore("PLAYER1", 10.05), "DAY2"),
      // Player 1 total score = 19.0
      Scores(SomeScore("PLAYER2", 10.00), "DAY1"),
      Scores(SomeScore("PLAYER2", 9.00), "DAY2"),
      // Player 2 total score = 19.0
      Scores(SomeScore("PLAYER3", 7.30), "DAY1"),
      Scores(SomeScore("PLAYER3", 3.70), "DAY2"),
      Scores(SomeScore("PLAYER4", 3.20), "DAY3"),
      Scores(SomeScore("PLAYER5", 4.30), "DAY3"),
      Scores(SomeScore("PLAYER6", 5.40), "DAY3")
    ))

    val playerTeamRdd: RDD[Game] = sc.parallelize(Seq(
      Team("PLAYER1", "TEAM1"),
      Team("PLAYER2", "TEAM1"),
      Team("PLAYER3", "TEAM1"),
      Team("PLAYER4", "TEAM2"),
      Team("PLAYER5", "TEAM2"),
      Team("PLAYER6", "TEAM3"),
      Team("PLAYER7", "TEAM3")))

    val rddRunner = new RDDRunner(sc)

    // Test multiple winning players
    val dataToTransform: Map[String, RDD[Game]] = Map("TEAMS" -> playerTeamRdd, "SCORES" -> playersScoreRdd)
    val transformed = rddRunner.transform(dataToTransform)
    println(transformed.collect().mkString)
    assert(transformed.collect.length === 2) // Expecting two lines of winners, One each for the winning teams and winning players
    assert(transformed.collect.head.score === 49) //Winning Team score should be 49

    //Test multiple winning teams
    val playersScoreRdd2: RDD[Game] = sc.parallelize(Seq(
      Scores(SomeScore("PLAYER1", 8.95), "DAY1"),
      Scores(SomeScore("PLAYER1", 10.05), "DAY2"),
      // Player 1 total score = 19.0
      Scores(SomeScore("PLAYER2", 10.00), "DAY1"),
      Scores(SomeScore("PLAYER2", 9.00), "DAY2"),
      // Player 2 total score = 19.0
      Scores(SomeScore("PLAYER3", 7.30), "DAY1"),
      Scores(SomeScore("PLAYER3", 3.70), "DAY2"),
      Scores(SomeScore("PLAYER4", 3.20), "DAY3"),
      Scores(SomeScore("PLAYER5", 4.30), "DAY3"),
      Scores(SomeScore("PLAYER6", 49.0), "DAY3")
    ))

    val dataToTransformMw = Map("TEAMS" -> playerTeamRdd, "SCORES" -> playersScoreRdd2)
    val transformedMw = rddRunner.transform(dataToTransformMw)
    println(transformedMw.collect().mkString)

    assert(transformedMw.collect().length === 2) // Expecting two entries of winners, One each for the winning teams and winning players
    assert(transformedMw.collect().head.score === transformedMw.collect()(1).score) //Both Winning Player and winning team have the same score

    val multiWinnerOutput = "src/test/resources/RddTestResultMw.dat"
    rddRunner.load(transformedMw, multiWinnerOutput)
  }

}
