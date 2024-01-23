package zio.query

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.{Chunk, ZIO}
import zio.query.BenchmarkUtil._

import java.util.concurrent.TimeUnit

@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class CollectAllBenchmark {

  @Param(Array("100", "1000"))
  var count: Int = 100

  val parallelism: Int = 10

  @Benchmark
  def zQueryCollectAll(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAll(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllFromRequest(): Long = {
    val queries = (0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)).toList
    val query   = ZQuery.collectAll(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllBatched(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllFromRequestBatched(): Long = {
    val queries = (0 until count).map(i => ZQuery.fromRequest(Req(i))(ds)).toList
    val query   = ZQuery.collectAllBatched(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllPar(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllPar(queries).map(_.sum.toLong)
    unsafeRun(query)
  }

  @Benchmark
  def zQueryCollectAllParN(): Long = {
    val queries = (0 until count).map(_ => ZQuery.succeed(1)).toList
    val query   = ZQuery.collectAllPar(queries).map(_.sum.toLong).withParallelism(parallelism)
    unsafeRun(query)
  }

  private case class Req(i: Int) extends Request[Nothing, Int]
  private val ds = DataSource.fromFunctionBatchedZIO("Datasource") { reqs: Chunk[Req] => ZIO.succeed(reqs.map(_.i)) }
}
