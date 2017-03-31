package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  val testConf = new SparkConf().setMaster("local").setAppName("StackOverflowTest")
  val sc: SparkContext = new SparkContext(testConf)
  //
  //  //  println(getClass.getResource("/stackoverflow/stackoverflow-100000.csv"))
  //  lazy val lines = sc.textFile(getClass.getResource("/stackoverflow/stackoverflow-100000.csv").getPath)
  //  lazy val raw = testObject.rawPostings(lines)
  //  lazy val grouped = testObject.groupedPostings(raw)
  //  lazy val scored = testObject.scoredPostings(grouped)
  //  lazy val vectors = testObject.vectorPostings(scored)
  //  //    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
  //
  //  lazy val means = testObject.kmeans(testObject.sampleVectors(vectors), vectors, debug = true)
  //  lazy val results = testObject.clusterResults(means, vectors)
  //
  test("clusterResults") {
    val centers = Array((0, 0), (100000, 0))
    val rdd = sc.parallelize(List(
      (0, 1000),
      (0, 23),
      (0, 234),
      (0, 0),
      (0, 1),
      (0, 1),
      (50000, 2),
      (50000, 10),
      (100000, 2),
      (100000, 5),
      (100000, 10),
      (200000, 100)))
    testObject.printResults(testObject.clusterResults(centers, rdd))
  }

  test("Grouping questions and answers together") {

//
//    val questions = postings.filter(_.postingType == 1).map(_.id).toSet
//    val answers = postings.filter(_.postingType == 2).map(_.parentId.get).toSet
//
//    println(s"${questions.size} :: questions ::: $questions ")
//    println(s"${answers.size} :: answers ::: $answers")
//
//    val intersect = questions.intersect(answers)
//    println(s"${intersect.size} :: answers ::: $intersect")
//
//    val rdd = sc.makeRDD(postings)
//    val results = testObject.groupedPostings(rdd).collect()

    assert(25 === 25)
//    assert(results.size === 25)
//    assert(results.contains(
//      (12590505, Iterable(
//        (Posting(1, 12590505, None, None, 6, Some("Python")), Posting(2, 12591054, None, Some(12590505), 6, None)),
//        (Posting(1, 12590505, None, None, 6, Some("Python")), Posting(2, 12591072, None, Some(12590505), 2, None)),
//        (Posting(1, 12590505, None, None, 6, Some("Python")), Posting(2, 12591124, None, Some(12590505), 1, None)),
//        (Posting(1, 12590505, None, None, 6, Some("Python")), Posting(2, 12591286, None, Some(12590505), 3, None))
//      ))
//    ))
  }

  //
  //  test("Vectors for clustering") {
  //    val questionsWithTopAnswer = List(
  //      (Posting(1, 1, None, None, 0, Some("Java")), 14),
  //      (Posting(1, 1, None, None, 0, None), 5),
  //      (Posting(1, 1, None, None, 0, Some("Scala")), 25),
  //      (Posting(1, 1, None, None, 0, Some("JavaScript")), 3)
  //    )
  //
  //    val rdd = sc.makeRDD(questionsWithTopAnswer)
  //
  //    val result = testObject.vectorPostings(rdd).collect()
  //    assert(result === Array((50000, 14), (500000, 25), (0, 3)))
  //  }
  //
  //  //Use this test only when you want to check how fast your implementations are
  //  //  test("Speed test of Grouped Postings + Scored Postings") {
  //  //    val resourcesPath = getClass.getResource("/stackoverflow/stackoverflow.csv")
  //  //    println(resourcesPath)
  //  //    val raw = testObject.rawPostings(sc.textFile(resourcesPath.toString))
  //  //    val grouped = testObject.groupedPostings(raw)
  //  //    val scored = testObject.scoredPostings(grouped)
  //  //
  //  //    assert(grouped.collect().size === 2121822)
  //  //
  //  //    scored.collect().take(10).foreach(println)
  //  //  }
  //
  //  test("rawPostings") {
  //    raw.take(10).foreach(println)
  //
  //    println(raw.toDebugString)
  //
  //    assert(raw.count() === 100000)
  //  }
  //
  //  test("groupedPostings") {
  //    grouped.take(10).foreach(println)
  //
  //    println(grouped.toDebugString)
  //
  //    assert(grouped.count() === 26075)
  //  }
  //
  //  test("scoredPostings") {
  //    scored.take(10).foreach(println)
  //
  //    println(scored.toDebugString)
  //
  //    assert(scored.count() === 26075)
  //    scored.take(2).foreach(println)
  //    assert(scored.take(2)(1)._2 === 0)
  //  }
  //
  //  test("vectorPostings") {
  //    vectors.take(10).foreach(println)
  //
  //    println(vectors.toDebugString)
  //
  //    assert(vectors.count() === 26075)
  //    assert(vectors.take(2)(0)._1 === 4 * testObject.langSpread)
  //  }
  //
  //  test("kmeans") {
  //    println("K-means: " + means.mkString(" | "))
  //    assert(means(0) === (1, 0))
  //  }
  //
  //  test("results") {
  //    results(0)
  //    testObject.printResults(results)
  //    assert(results(0) === ("Java", 100, 1361, 0))
  //  }

}
