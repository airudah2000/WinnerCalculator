import java.util.logging.Level

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

trait TestUtil {
  final val appName = "Unit-Test"
  final val master = "local[*]"
  final val conf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
  final val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()
  final val sc = spark.sparkContext
  sc.setLogLevel(Level.OFF.toString)
}
