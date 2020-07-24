package fsp

import fsp.Main.DsTl
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class MainTest extends AnyFunSuite with Matchers {

  test("one date") {
    Main.toIntDate("02.01.2013").mustBe(1)
    Main.toIntDate("03.01.2013").mustBe(2)
  }


  test("merge zeros") {
    val l = List(DsTl(5L, 1.5), DsTl(2L, 0.5))
    val l1 = Main.mergeZeros(l, 0, 6)
    l1.mkString(",").mustBe("DsTl(0,0.0),DsTl(1,0.0),DsTl(2,0.5),DsTl(3,0.0),DsTl(4,0.0),DsTl(5,1.5),DsTl(6,0.0)")
  }
  test("merge zeros empty") {
    val l = List.empty[DsTl]
    val l1 = Main.mergeZeros(l, 0, 0)
    l1.mkString(",").mustBe("DsTl(0,0.0)")
  }
}
