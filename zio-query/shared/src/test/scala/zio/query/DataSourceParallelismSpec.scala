package zio.query

import zio.test._
import zio._

object DataSourceParallelismSpec extends ZIOSpecDefault {
  def spec = suite("DataSourceParallelismSpec")(
    test("zipPar works with flatmapped datasources") {
      val q1 = ds1.query(Req1(1))
      val q2 = ds2.query(Req2(2)).flatMap(_ => ds3.query(Req3(3)))
      (q1 zipPar q2).run.as(assertCompletes)
    },
    test("zipPar works with flatmapped datasources for both queries") {
      val q1 = ds1.query(Req1(1)).flatMap(_ => ds2.query(Req2(1)))
      val q2 = ds2.query(Req2(2)).flatMap(_ => ds3.query(Req3(3)))
      (q1 zipPar q2).run.as(assertCompletes)
    },
    test("collectAllPar works with flatmapped datasources") {
      val q1 = ds1.query(Req1(1)).flatMap(_ => ds2.query(Req2(1)))
      val q2 = ds2.query(Req2(2)).flatMap(_ => ds3.query(Req3(3)))
      ZQuery.collectAllPar(List(q1, q2)).run.as(assertCompletes)
    },
    test("collectAllPar works with flatmapped datasources for both queries") {
      val q1 = ds1.query(Req1(1))
      val q2 = ds2.query(Req2(2)).flatMap(_ => ds3.query(Req3(3)))
      ZQuery.collectAllPar(List(q1, q2)).run.as(assertCompletes)
    }
  ).provideLayer(ZLayer(Promise.make[Nothing, Unit]))

  private val ds1 = DataSource.fromFunctionZIO[Promise[Nothing, Unit], Nothing, Req1, Int]("ds1") { case Req1(i) =>
    ZIO.serviceWithZIO[Promise[Nothing, Unit]](_.await).as(i)
  }
  private val ds2 = DataSource.fromFunctionZIO[Any, Nothing, Req2, Int]("ds2") { case Req2(i) =>
    ZIO.succeed(i)
  }
  private val ds3 = DataSource.fromFunctionZIO[Promise[Nothing, Unit], Nothing, Req3, Int]("ds3") { case Req3(i) =>
    ZIO.serviceWithZIO[Promise[Nothing, Unit]](_.succeed(())).as(i)
  }

  case class Req1(i: Int) extends Request[Nothing, Int]
  case class Req2(i: Int) extends Request[Nothing, Int]
  case class Req3(i: Int) extends Request[Nothing, Int]
}
