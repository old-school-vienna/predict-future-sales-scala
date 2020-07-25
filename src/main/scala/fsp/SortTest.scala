package fsp

import entelijan.viz.Viz._
import entelijan.viz._
import org.apache.spark.sql.SparkSession

import scala.util.Random._

case class T(
              x: Int,
              v: Double,
            )

case class TG(
              g: String,
              x: Int,
              v: Double,
            )

object SortTest extends App {

  noSparkUngrouped()
  
  def createDataGrouped(): Seq[Seq[T]] =
    for (j <- 10 to 30) yield {
      for (i <- 1 to 50) yield {
        val d = (nextInt(11) - 5) * 0.3
        T(i, math.max(j + d, 0.0))
      }
    }

  def createDataUngrouped(): Seq[TG] ={
    val groups = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I" )
    for (j <- groups.indices; i <- 1 to 10) yield {
      val d = (nextInt(11) - 5) * 0.3
      TG(groups(j),i, math.max(j + 5  + d, 0.0))
    }
  }
    


  private def noSparkUngrouped(): Unit = {
    val rows = createDataUngrouped()
      .groupBy(x => x.g)
      .toList
      .map(t => t._2)
      .map(l => (l.map(t => t.v).sum, l))
      .sortBy { case (l, _) => -l }
      .map(t => t._2)
      .zipWithIndex
      .map(t => DataRow(
        data = t._1.map(x => XYZ(t._2, x.x, x.v)),
        style = Style_BOXES))

    println(rows.mkString("\n"))
    val dia = Diagram[XYZ](
      xLabel = Some("x"),
      yLabel = Some("y"),
      id = "sort_test_ungrouped",
      title = "Sort Test ungrouped",

      xyPlaneAt = Some(0),
      dataRows = rows,
    )
    val crea: VizCreator[XYZ] = VizCreators.gnuplot(clazz = classOf[XYZ])
    crea.createDiagram(dia)
  }


  private def noSparkGrouped(): Unit = {

    val ts: Seq[Seq[T]] = createDataGrouped()
    
    val rows = ts
      .map(l => (l.map(t => t.v).sum, l))
      .sortBy { case (l, _) => -l }
      .map(t => t._2)
      .zipWithIndex
      .map(t => DataRow(
        data = t._1.map(x => XYZ(t._2, x.x, x.v)),
        style = Style_BOXES))

    println(rows.mkString("\n"))

    val dia = Diagram[XYZ](
      xLabel = Some("x"),
      yLabel = Some("y"),
      id = "sort_test",
      title = "Sort Test",

      xyPlaneAt = Some(0),
      dataRows = rows.toList,
    )

    val crea: VizCreator[XYZ] = VizCreators.gnuplot(clazz = classOf[XYZ])
    crea.createDiagram(dia)
  }

  private def sparkGrouped(): Unit = {

    val spark = SparkSession.builder().appName("main").master("local").getOrCreate()

    val ts = createDataGrouped()
    val tss = spark.sparkContext.parallelize(ts)
    
    val rows = tss
      .map(l => (l.map(t => t.v).sum, l))
      .sortBy { case (l, _) => -l }
      .map(t => t._2)
      .zipWithIndex
      .map(t => DataRow(
        data = t._1.map(x => XYZ(t._2, x.x, x.v)),
        style = Style_BOXES))
      .toLocalIterator
      .toList

    println(rows.mkString("\n"))

    val dia = Diagram[XYZ](
      xLabel = Some("x"),
      yLabel = Some("y"),
      id = "sort_test",
      title = "Sort Test",

      xyPlaneAt = Some(0),
      dataRows = rows,
    )

    val crea: VizCreator[XYZ] = VizCreators.gnuplot(clazz = classOf[XYZ])
    crea.createDiagram(dia)
  }

}
