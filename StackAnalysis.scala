package com.analysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

object StackAnalysis {
  def main (args: Array[String]) {
    val options = CommandLineOptions.getMap(args)

    val conf = new SparkConf().setMaster("local[*]").setAppName("StackAnalysis")
    val sc = new SparkContext(conf)

    try {
      val inputFile = options.getOrElse('inputfile, "data/stackPosts100k.xml").toString

      val outputDirOption = options.get('outputdir)

      try {
        val rows = sc.textFile(inputFile)
        val sqs = scalaQuestions(rows)

        println(s"Scala question count ${sqs.count()}")

        val tc = tagCounts(sqs)

        val sqsByMonth = scalaQuestionsByMonth(sqs)

        outputDirOption match {
          case Some(outputDir) ?
            println("Writing output files to disk")

            val outputDirStr = outputDir.toString
            val stcFile = s"$outputDirStr/ScalaTagCount.txt"
            val sqbmFile = s"$outputDirStr/ScalaQuestionsByMonth.txt"

            tc.saveAsTextFile(stcFile)
            sqsByMonth.saveAsTextFile(sqbmFile)
          case None ? println("No output file provided.")
        }
      } finally {
        sc.stop()
      }
    }
  }

  def scalaQuestions(rows: RDD[String]) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    rows
      .filter(l ? l.contains("row"))
      .flatMap(l ? {
        try {
          val xml = XML.loadString(l)
          val postTypeId = (xml \ "@PostTypeId").text.toInt
          val creationDate = (xml \ "@CreationDate").text
          val tags = (xml \ "@Tags").text
          List((postTypeId, creationDate, tags))
        } catch {
          case ex: Exception ?
            println(s"failed to parse line: $l")
            Nil
        }
      })
      .map { case (postTypeId, creationDateString, tagString) ? (postTypeId, sdf.parse(creationDateString), tagString) }
      .map { case (postTypeId, creationDate, tagString) ?
        val splitTags = if (tagString.length == 0) Array[String]() else tagString.substring(1,tagString.length-1).split("><")
        (postTypeId, creationDate, splitTags.toList)
      }
     
      .filter { case (postTypeId, creationDate, tags) ? postTypeId == 1 }
     
      .filter { case (id, creationDate, tags) ? tags.contains("scala") }
      .cache()
  }

  def tagCounts(qs: RDD[(Int, Date, List[String])]) = {
    qs
      .flatMap { case (postTypeId, creationDate, tags) ?
        val otherTags = tags.diff(List("scala"))
        if (tags.length > otherTags.length)
          otherTags.map(tag ? (tag, 1))
        else
          Nil
      }
      .reduceByKey((a, b) ? a + b)
      .map { case (tag, count) ? (count, tag) }
      .sortByKey(ascending = false)
  }

  def scalaQuestionsByMonth(sqs: RDD[(Int, Date, List[String])]) = {
    val sdfMonth = new SimpleDateFormat("yyyy-MM")

    sqs
      .map { case (id, creationDate, tags) ? (sdfMonth.format(creationDate), 1)}
      .reduceByKey((a, b) ? a + b)
      .sortByKey(ascending = true)
  }
}