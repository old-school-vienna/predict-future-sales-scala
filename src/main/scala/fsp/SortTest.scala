package fsp

import entelijan.viz.Viz._
import entelijan.viz._

import scala.util.Random._

case class T(
              x: Int,
              v: Double,
            )

object SortTest extends App {

  noSpark()


  private def noSpark(): Unit = {

    val ts: Seq[Seq[T]] = for (j <- 10 to 30) yield {
      val x: Seq[Option[T]] = for (i <- 1 to 50) yield {
        val d = (nextInt(11) - 5) * 0.3
        if (nextInt(100) < 10) Some(T(i, math.max(j + d, 0.0)))
        else None
      }
      x.flatten
    }
    
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

}
