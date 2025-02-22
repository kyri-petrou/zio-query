package zio.query

import zio._
import zio.query.QueryAspect._
import zio.test.Assertion._
import zio.test.TestAspect.{after, nonFlaky, silent}
import zio.test.{TestClock, TestConsole, TestEnvironment, _}

object ZQuerySpec extends ZIOBaseSpec {

  override def spec: Spec[TestEnvironment, Any] =
    suite("ZQuerySpec")(
      test("N + 1 selects problem") {
        for {
          _   <- getAllUserNames.run
          log <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2)))
      },
      test("mapError does not prevent batching") {
        implicit val canFail = zio.CanFail
        val a                = getUserNameById(1).zip(getUserNameById(2)).mapError(identity)
        val b                = getUserNameById(3).zip(getUserNameById(4)).mapError(identity)
        for {
          _   <- ZQuery.collectAllPar(List(a, b)).run
          log <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2)))
      },
      test("failure to complete request is query failure") {
        for {
          result <- getUserNameById(27).run.exit
        } yield assert(result)(dies(equalTo(QueryFailure(UserRequestDataSource, GetNameById(27)))))
      },
      test("query failure is correctly reported") {
        val failure = QueryFailure(UserRequestDataSource, GetNameById(27))
        assert(failure.getMessage)(
          equalTo("Data source UserRequestDataSource did not complete request GetNameById(27).")
        )
      },
      test("timed does not prevent batching") {
        val a = getUserNameById(1).zip(getUserNameById(2)).timed
        val b = getUserNameById(3).zip(getUserNameById(4))
        for {
          _   <- ZQuery.collectAllPar(List(a, b)).run
          log <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2)))
      },
      test("optional converts a query to one that returns its value optionally") {
        for {
          result <- getUserNameById(27).map(identity).optional.run
        } yield assert(result)(isNone)
      },
      suite("zip")(
        test("arbitrary effects are executed in order") {
          for {
            ref    <- Ref.make(List.empty[Int])
            query1  = ZQuery.fromZIO(ref.update(1 :: _))
            query2  = ZQuery.fromZIO(ref.update(2 :: _))
            _      <- (query1 *> query2).run
            result <- ref.get
          } yield assert(result)(equalTo(List(2, 1)))
        } @@ nonFlaky,
        test("requests are executed in order") {
          val query = Cache.put(0, 1) *> Cache.getAll <* Cache.put(1, -1)
          assertZIO(query.run)(equalTo(Map(0 -> 1)))
        } @@ after(Cache.clear) @@ nonFlaky,
        test("requests are executed in order after parallel execution") {
          val query =
            (Cache.putIfAbsent(0, 0) &> Cache.putIfAbsent(1, 1)) *>
              Cache.getAll <*
              (Cache.putIfAbsent(2, -1) &> Cache.putIfAbsent(3, -1))
          assertZIO(query.run)(equalTo(Map(0 -> 0, 1 -> 1)))
        } @@ after(Cache.clear) @@ nonFlaky,
        test("requests are pipelined") {
          val query = Cache.put(0, 1) *> Cache.getAll <* Cache.put(1, -1)
          assertZIO(query.run *> Cache.log)(hasSize(equalTo(1)))
        } @@ after(Cache.clear) @@ nonFlaky,
        test("intervening flatMap prevents pipelining") {
          val query = Cache.put(0, 1).flatMap(ZQuery.succeed(_)) *> Cache.getAll <* Cache.put(1, -1)
          assertZIO(query.run *> Cache.log)(hasSize(equalTo(2)))
        } @@ after(Cache.clear) @@ nonFlaky,
        test("trailing flatMap does not prevent pipelining") {
          val query = Cache.put(0, 1) *> Cache.getAll <* Cache.put(1, -1).flatMap(ZQuery.succeed(_))
          assertZIO(query.run *> Cache.log)(hasSize(equalTo(1)))
        } @@ after(Cache.clear) @@ nonFlaky,
        test("short circuits on failure") {
          for {
            ref    <- Ref.make(true)
            query   = ZQuery.fail("fail") *> ZQuery.fromZIO(ref.set(false))
            _      <- query.run.ignore
            result <- ref.get
          } yield assert(result)(isTrue)
        } @@ nonFlaky,
        test("does not deduplicate uncached requests") {
          val query = Cache.getAll *> Cache.put(0, 1) *> Cache.getAll
          assertZIO(query.uncached.run)(equalTo(Map(0 -> 1)))
        } @@ nonFlaky
      ).provideCustom(Cache.live),
      suite("zipBatched")(
        test("queries to multiple data sources can be executed in parallel") {
          for {
            promise <- Promise.make[Nothing, Unit]
            _       <- (neverQuery.zipBatched(succeedQuery(promise))).run.fork
            _       <- promise.await
          } yield assertCompletes
        },
        test("arbitrary effects are executed in order") {
          for {
            ref    <- Ref.make(List.empty[Int])
            query1  = ZQuery.fromZIO(ref.update(1 :: _))
            query2  = ZQuery.fromZIO(ref.update(2 :: _))
            _      <- (query1.zipBatchedRight(query2)).run
            result <- ref.get
          } yield assert(result)(equalTo(List(2, 1)))
        } @@ nonFlaky
      ),
      suite("zipPar")(
        test("queries to multiple data sources can be executed in parallel") {
          for {
            promise <- Promise.make[Nothing, Unit]
            _       <- (neverQuery <&> succeedQuery(promise)).run.fork
            _       <- promise.await
          } yield assertCompletes
        },
        test("arbitrary effects can be executed in parallel") {
          for {
            promise <- Promise.make[Nothing, Unit]
            _       <- (ZQuery.never <&> ZQuery.fromZIO(promise.succeed(()))).run.fork
            _       <- promise.await
          } yield assertCompletes
        },
        test("does not prevent batching") {
          for {
            _   <- ZQuery.collectAllPar(List.fill(100)(getAllUserNames)).run
            log <- TestConsole.output
          } yield assert(log)(hasSize(equalTo(2)))
        } @@ nonFlaky
      ),
      test("stack safety") {
        val effect = (0 to 100000)
          .map(ZQuery.succeed(_))
          .foldLeft(ZQuery.succeed(0)) { (query1, query2) =>
            for {
              acc <- query1
              i   <- query2
            } yield acc + i
          }
          .run
        assertZIO(effect)(equalTo(705082704))
      },
      test("data sources can be raced") {
        for {
          promise <- Promise.make[Nothing, Unit]
          _       <- raceQuery(promise).run
          _       <- promise.await
        } yield assertCompletes
      },
      test("max batch size") {
        val query = getAllUserNames @@ maxBatchSize(3)
        for {
          result <- query.run
          log    <- TestConsole.output
        } yield assert(result)(hasSameElements(userNames.values)) &&
          assert(log)(hasSize(equalTo(10)))
      },
      test("multiple data sources do not prevent batching") {
        for {
          _   <- ZQuery.collectAllPar(List(getFoo, getBar)).run
          log <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2)))
      },
      test("efficiency of large queries") {
        val query = for {
          users <- ZQuery.fromZIO(
                     ZIO.succeed(
                       List.tabulate(Sources.totalCount)(id => User(id, "user name", id, id))
                     )
                   )
          richUsers <- ZQuery.foreachPar(users) { user =>
                         Sources
                           .getPayment(user.paymentId)
                           .zip(Sources.getAddress(user.addressId))
                           .map { case (payment, address) =>
                             (user, payment, address)
                           }
                       }
        } yield richUsers.size
        assertZIO(query.run)(equalTo(Sources.totalCount))
      },
      test("data sources can return additional results") {
        val getSome = ZQuery.foreachPar(List(3, 4))(get).map(_.toSet)
        val query   = getAll *> getSome
        for {
          result <- query.run
          output <- TestConsole.output
        } yield assert(result)(equalTo(Set("c", "d"))) &&
          assert(output)(equalTo(Vector("getAll called\n")))
      },
      suite("caching results fulfilled by datasources")(
        test("caching enabled") {
          for {
            cache <- zio.query.Cache.empty
            query = for {
                      res <- ZQuery.fromRequest(Req.Get(1))(dsCompletingMoreRequests)
                    } yield res
            requestResult <- query.runCache(cache)
            oneToTen       = (1 to 10).toList
            cachedResults <- ZIO.foreach(oneToTen)(i => cache.get(Req.Get(i)).flatMap(_.await))
            cacheCheck     = cachedResults == oneToTen.map(_.toString)
          } yield assertTrue(requestResult == "1", cacheCheck)
        },
        test("caching disabled") {
          for {
            cache <- zio.query.Cache.empty
            query = for {
                      res <- ZQuery.fromRequestUncached(Req.Get(1))(dsCompletingMoreRequests)
                    } yield res
            requestResult <- query.runCache(cache)
            oneToTen       = (1 to 10).toList
            cachedResults <- ZIO.foreach(oneToTen)(i => cache.get(Req.Get(i)).isFailure)
            cacheCheck     = cachedResults.forall(identity)
          } yield assertTrue(requestResult == "1", cacheCheck)
        }
      ),
      test("requests can be removed from the cache") {
        for {
          cache <- zio.query.Cache.empty
          query = for {
                    _ <- getUserNameById(1)
                    _ <- ZQuery.fromZIO(cache.remove(GetNameById(1)))
                    _ <- getUserNameById(1)
                  } yield ()
          _   <- query.runCache(cache)
          log <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2)))
      },
      suite("timeout")(
        test("times out a query that does not complete") {
          for {
            fiber <- ZQuery.never.timeout(1.second).run.fork
            _     <- TestClock.adjust(1.second)
            _     <- fiber.join
          } yield assertCompletes
        },
        test("prevents subsequent requests to data sources from being executed") {
          for {
            fiber <- (ZQuery.fromZIO(ZIO.sleep(2.seconds)) *> neverQuery).timeout(1.second).run.fork
            _     <- TestClock.adjust(2.second)
            _     <- fiber.join
          } yield assertCompletes
        }
      ),
      test("regional caching should work with parallelism") {
        val left = for {
          _ <- getUserNameById(1)
          _ <- ZQuery.fromZIO(ZIO.sleep(1000.milliseconds))
          _ <- getUserNameById(1)
        } yield ()
        val right = for {
          _ <- getUserNameById(2)
          _ <- ZQuery.fromZIO(ZIO.sleep(500.milliseconds))
        } yield ()
        val query = left.uncached.zipPar(right.cached)
        for {
          fiber <- query.run.fork
          _     <- TestClock.adjust(500.milliseconds)
          _     <- TestClock.adjust(1000.milliseconds)
          _     <- fiber.join
          log   <- TestConsole.output
        } yield assert(log)(hasSize(equalTo(2))) &&
          assert(log)(hasAt(0)(containsString("GetNameById(1)"))) &&
          assert(log)(hasAt(0)(containsString("GetNameById(2)"))) &&
          assert(log)(hasAt(1)(containsString("GetNameById(1)")))
      } @@ nonFlaky,
      suite("race")(
        test("race with never") {
          val query = ZQuery.never.race(ZQuery.succeed(()))
          assertZIO(query.run)(anything)
        },
        test("interruption of loser") {
          for {
            promise1 <- Promise.make[Nothing, Unit]
            promise2 <- Promise.make[Nothing, Unit]
            left      = ZQuery.fromZIO((promise1.succeed(()) *> ZIO.never).onInterrupt(promise2.succeed(())))
            right     = ZQuery.fromZIO(promise1.await)
            _        <- left.race(right).run
            _        <- promise2.await
          } yield assertCompletes
        }
      ) @@ nonFlaky,
      suite("around data source aspect")(
        test("wraps data source with before and after effects that are evaluated accordingly") {
          for {
            beforeRef <- Ref.make(0)
            before     = beforeRef.set(1) *> beforeRef.get

            afterRef    <- Ref.make(0)
            after        = (v: Int) => afterRef.set(v * 2)
            aspect       = QueryAspect.aroundDataSource(Described(before, "before effect"))(Described(after, "after effect"))
            query        = getUserNameById(1) @@ aspect
            _           <- query.run
            isBeforeRan <- beforeRef.get
            isAfterRan  <- afterRef.get
          } yield assert(isBeforeRan)(equalTo(1)) && assert(isAfterRan)(equalTo(2))
        }
      ) @@ nonFlaky,
      test("service methods works with multiple services") {
        def getFoo: ZQuery[Int with String, Nothing, Unit] =
          ZQuery.serviceWithQuery[Int](_ => ZQuery.service[String].as(()))

        def getBar: ZQuery[Int with String, Nothing, Unit] =
          ZQuery.serviceWithZIO[String](_ => ZIO.service[String].unit)

        assertCompletes
      },
      test("acquireReleaseWith") {
        def query(n: Int): ZQuery[Cache, Nothing, Unit] =
          if (n == 0) ZQuery.unit
          else ZQuery.fromZIO(Random.nextInt).flatMap(Cache.get(_).flatMap(_ => query(n - 1)))
        for {
          ref    <- Ref.make(0)
          acquire = ref.update(_ + 1)
          release = ref.update(_ - 1)
          fiber <- ZQuery
                     .acquireReleaseWith(acquire)(_ => release)(_ => query(100))
                     .run
                     .fork
          _     <- fiber.interrupt
          value <- ref.get
        } yield assertTrue(value == 0)
      }.provideLayer(Cache.live) @@ nonFlaky,
      test("defect in data source is translated to defect in request") {
        val query = dieQuery.foldCauseQuery(_ => ZQuery.unit, _ => ZQuery.unit)
        for {
          _ <- query.run
        } yield assertCompletes
      }
    ) @@ silent

  val userIds: List[Int]          = (1 to 26).toList
  val userNames: Map[Int, String] = userIds.zip(('a' to 'z').map(_.toString)).toMap

  sealed trait UserRequest[A] extends Request[Nothing, A]

  case object GetAllIds                 extends UserRequest[List[Int]]
  final case class GetNameById(id: Int) extends UserRequest[String]

  val UserRequestDataSource: DataSource[Any, UserRequest[_]] =
    DataSource.Batched.make[Any, UserRequest[_]]("UserRequestDataSource") { requests =>
      ZIO.when(requests.toSet.size != requests.size)(ZIO.dieMessage("Duplicate requests)")) *>
        Console.printLine(requests.toString).orDie *>
        ZIO.succeed {
          requests.foldLeft(CompletedRequestMap.empty) {
            case (completedRequests, GetAllIds) => completedRequests.insert(GetAllIds, Exit.succeed(userIds))
            case (completedRequests, GetNameById(id)) =>
              userNames
                .get(id)
                .fold(completedRequests)(name => completedRequests.insert(GetNameById(id), Exit.succeed(name)))
          }
        }
    }

  val getAllUserIds: ZQuery[Any, Nothing, List[Int]] =
    ZQuery.fromRequest(GetAllIds)(UserRequestDataSource)

  def getUserNameById(id: Int): ZQuery[Any, Nothing, String] =
    ZQuery.fromRequest(GetNameById(id))(UserRequestDataSource)

  val getAllUserNames: ZQuery[Any, Nothing, List[String]] =
    for {
      userIds   <- getAllUserIds
      userNames <- ZQuery.foreachPar(userIds)(getUserNameById)
    } yield userNames

  case object GetFoo extends Request[Nothing, String]
  val getFoo: ZQuery[Any, Nothing, String] = ZQuery.fromRequest(GetFoo)(
    DataSource.fromFunctionZIO("foo")(_ => Console.printLine("Running foo query").orDie *> ZIO.succeed("foo"))
  )

  case object GetBar extends Request[Nothing, String]
  val getBar: ZQuery[Any, Nothing, String] = ZQuery.fromRequest(GetBar)(
    DataSource.fromFunctionZIO("bar")(_ => Console.printLine("Running bar query").orDie *> ZIO.succeed("bar"))
  )

  case object NeverRequest extends Request[Nothing, Nothing]

  val neverQuery: ZQuery[Any, Nothing, Nothing] =
    ZQuery.fromRequest(NeverRequest)(DataSource.never)

  final case class SucceedRequest(promise: Promise[Nothing, Unit]) extends Request[Nothing, Unit]

  val succeedDataSource: DataSource[Any, SucceedRequest] =
    DataSource.fromFunctionZIO("succeed") { case SucceedRequest(promise) =>
      promise.succeed(()).unit
    }

  def succeedQuery(promise: Promise[Nothing, Unit]): ZQuery[Any, Nothing, Unit] =
    ZQuery.fromRequest(SucceedRequest(promise))(succeedDataSource)

  val raceDataSource: DataSource[Any, SucceedRequest] =
    DataSource.never.race(succeedDataSource)

  def raceQuery(promise: Promise[Nothing, Unit]): ZQuery[Any, Nothing, Unit] =
    ZQuery.fromRequest(SucceedRequest(promise))(raceDataSource)

  case object DieRequest extends Request[Nothing, Nothing]

  val dieDataSource: DataSource[Any, DieRequest.type] =
    new DataSource[Any, DieRequest.type] {
      val identifier: String = "die"

      def runAll(requests: Chunk[Chunk[DieRequest.type]])(implicit
        trace: Trace
      ): ZIO[Any, Nothing, CompletedRequestMap] =
        ZIO.dieMessage("die")
    }

  val dieQuery: ZQuery[Any, Nothing, Nothing] =
    ZQuery.fromRequest(DieRequest)(dieDataSource)

  sealed trait CacheRequest[A] extends Request[Nothing, A]

  final case class Get(key: Int)                     extends CacheRequest[Option[Int]]
  case object GetAll                                 extends CacheRequest[Map[Int, Int]]
  final case class Put(key: Int, value: Int)         extends CacheRequest[Unit]
  final case class PutIfAbsent(key: Int, value: Int) extends CacheRequest[Unit]

  type Cache = Cache.Service

  object Cache {

    trait Service extends DataSource[Any, CacheRequest[_]] {
      val clear: ZIO[Any, Nothing, Unit]
      val log: ZIO[Any, Nothing, List[List[Set[CacheRequest[_]]]]]
    }

    val live: ZLayer[Any, Throwable, Cache] =
      ZLayer.fromZIO {
        for {
          cache <- Ref.make(Map.empty[Int, Int])
          ref   <- Ref.make[List[List[Set[CacheRequest[_]]]]](Nil)
        } yield new Service {
          val clear: ZIO[Any, Nothing, Unit] =
            cache.set(Map.empty) *> ref.set(List.empty)
          val log: ZIO[Any, Nothing, List[List[Set[CacheRequest[_]]]]] =
            ref.get
          val identifier: String =
            "CacheDataSource"
          def runAll(requests: Chunk[Chunk[CacheRequest[_]]])(implicit
            trace: Trace
          ): ZIO[Any, Nothing, CompletedRequestMap] =
            ref.update(requests.map(_.toSet).toList :: _) *>
              ZIO
                .foreach(requests) { requests =>
                  ZIO
                    .foreachPar(requests) {
                      case Get(key) =>
                        cache.get.map(_.get(key)).exit.map(CompletedRequestMap.empty.insert(Get(key), _))
                      case GetAll =>
                        cache.get.exit.map(CompletedRequestMap.empty.insert(GetAll, _))
                      case Put(key, value) =>
                        cache.update(_ + (key -> value)).exit.map(CompletedRequestMap.empty.insert(Put(key, value), _))
                      case PutIfAbsent(key, value) =>
                        cache.get.flatMap { map =>
                          if (map.contains(key)) ZIO.die(new Exception(s"Expected key $key to be absent from cache"))
                          else
                            cache
                              .update(_ + (key -> value))
                              .exit
                              .map(CompletedRequestMap.empty.insert(PutIfAbsent(key, value), _))
                        }
                    }
                    .map(_.foldLeft(CompletedRequestMap.empty)(_ ++ _))
                }
                .map(_.foldLeft(CompletedRequestMap.empty)(_ ++ _))
        }
      }

    def get(key: Int): ZQuery[Cache, Nothing, Option[Int]] =
      for {
        cache <- ZQuery.environment[Cache].map(_.get)
        value <- ZQuery.fromRequest(Get(key))(cache)
      } yield value

    val getAll: ZQuery[Cache, Nothing, Map[Int, Int]] =
      for {
        cache <- ZQuery.environment[Cache].map(_.get)
        value <- ZQuery.fromRequest(GetAll)(cache)
      } yield value

    def put(key: Int, value: Int): ZQuery[Cache, Nothing, Unit] =
      for {
        cache <- ZQuery.environment[Cache].map(_.get)
        value <- ZQuery.fromRequest(Put(key, value))(cache)
      } yield value

    def putIfAbsent(key: Int, value: Int): ZQuery[Cache, Nothing, Unit] =
      for {
        cache <- ZQuery.environment[Cache].map(_.get)
        value <- ZQuery.fromRequest(PutIfAbsent(key, value))(cache)
      } yield value

    val clear: ZIO[Cache, Nothing, Unit] =
      ZIO.serviceWithZIO(_.clear)

    val log: ZIO[Cache, Nothing, List[List[Set[CacheRequest[_]]]]] =
      ZIO.serviceWithZIO(_.log)
  }

  case class Bearer(value: String)

  case class User(id: Int, name: String, addressId: Int, paymentId: Int)
  case class Address(id: Int, street: String)
  case class Payment(id: Int, name: String)

  object Sources {

    val totalCount = 15000

    val paymentData: Map[Int, Payment] = List.tabulate(totalCount)(i => i -> Payment(i, "payment name")).toMap
    case class GetPayment(id: Int) extends Request[Nothing, Payment]
    val paymentSource: DataSource[Any, GetPayment] =
      DataSource.fromFunctionBatchedOptionZIO("PaymentSource") { (requests: Chunk[GetPayment]) =>
        ZIO.succeed(requests.map(req => paymentData.get(req.id)))
      }

    def getPayment(id: Int): UQuery[Payment] =
      ZQuery.fromRequest(GetPayment(id))(paymentSource)

    val addressData: Map[Int, Address] = List.tabulate(totalCount)(i => i -> Address(i, "street")).toMap
    case class GetAddress(id: Int) extends Request[Nothing, Address]
    val addressSource: DataSource[Any, GetAddress] =
      DataSource.fromFunctionBatchedOptionZIO("AddressSource") { (requests: Chunk[GetAddress]) =>
        ZIO.succeed(requests.map(req => addressData.get(req.id)))
      }

    def getAddress(id: Int): UQuery[Address] =
      ZQuery.fromRequest(GetAddress(id))(addressSource)
  }

  val testData: Map[Int, String] = Map(
    1 -> "a",
    2 -> "b",
    3 -> "c",
    4 -> "d"
  )

  def backendGetAll: ZIO[Any, Nothing, Map[Int, String]] =
    for {
      _ <- Console.printLine("getAll called").orDie
    } yield testData

  def backendGetSome(ids: Chunk[Int]): ZIO[Any, Nothing, Map[Int, String]] =
    for {
      _ <- Console.printLine(s"getSome ${ids.mkString(", ")} called").orDie
    } yield ids.flatMap { id =>
      testData.get(id).map(v => id -> v)
    }.toMap

  sealed trait DataSourceErrors
  case class NotFound(id: Int) extends DataSourceErrors

  sealed trait Req[A] extends Request[DataSourceErrors, A]
  object Req {
    case object GetAll            extends Req[Map[Int, String]]
    final case class Get(id: Int) extends Req[String]
  }

  val ds: DataSource.Batched[Any, Req[_]] = new DataSource.Batched[Any, Req[_]] {
    override def run(
      requests: Chunk[Req[_]]
    )(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] = {
      val (all, oneByOne) = requests.partition {
        case Req.GetAll => true
        case Req.Get(_) => false
      }

      if (all.nonEmpty) {
        backendGetAll.map { allItems =>
          allItems
            .foldLeft(CompletedRequestMap.empty) { case (result, (id, value)) =>
              result.insert(Req.Get(id), Exit.succeed(value))
            }
            .insert(Req.GetAll, Exit.succeed(allItems))
        }
      } else {
        for {
          items <- backendGetSome(oneByOne.flatMap {
                     case Req.GetAll  => Chunk.empty
                     case Req.Get(id) => Chunk(id)
                   })
        } yield oneByOne.foldLeft(CompletedRequestMap.empty) {
          case (result, Req.GetAll) => result
          case (result, req @ Req.Get(id)) =>
            items.get(id) match {
              case Some(value) => result.insert(req, Exit.succeed(value))
              case None        => result.insert(req, Exit.fail(NotFound(id)))
            }
        }
      }
    }

    override val identifier: String = "test"
  }

  val dsCompletingMoreRequests: DataSource.Batched[Any, Req.Get] = new DataSource.Batched[Any, Req.Get] {
    override def run(
      requests: Chunk[Req.Get]
    )(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] =
      ZIO.succeed(
        CompletedRequestMap.fromIterable(
          (1 to 10).map(id => Req.Get(id) -> Exit.succeed(id.toString))
        )
      )

    override val identifier: String = "test"
  }

  def getAll: ZQuery[Any, DataSourceErrors, Map[Int, String]] =
    ZQuery.fromRequest(Req.GetAll)(ds)
  def get(id: Int): ZQuery[Any, DataSourceErrors, String] =
    ZQuery.fromRequest(Req.Get(id))(ds)
}
