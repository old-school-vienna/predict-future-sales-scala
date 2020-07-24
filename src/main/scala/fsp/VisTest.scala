package fsp

import entelijan.viz.Viz.{DataRow, Diagram, XYZ}
import entelijan.viz.{VizCreator, VizCreators}

object VisTest extends App {

  def rows = {
    for (i <- -10 to 10) yield
      DataRow(data = for (j <- 1 to 40) yield
        XYZ(i, j, math.exp(i * j * 0.01)))
  }


  println(rows.mkString("\n"))

  val dia = Diagram[XYZ](
    id = "pfs_test",
    title = "predict future sales",
    xyPlaneAt = Some(0),
    dataRows =rows,
  )

  val crea: VizCreator[XYZ] = VizCreators.gnuplot(clazz = classOf[XYZ])
  crea.createDiagram(dia)


}
