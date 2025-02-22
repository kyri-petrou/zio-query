/*
 * Copyright 2019-2023 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.query

import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.{Chunk, Exit, Trace, ZEnvironment, ZIO}

/**
 * A `DataSource[R, A]` requires an environment `R` and is capable of executing
 * requests of type `A`.
 *
 * Data sources must implement the method `runAll` which takes a collection of
 * requests and returns an effect with a `CompletedRequestMap` containing a
 * mapping from requests to results. The type of the collection of requests is a
 * `Chunk[Chunk[A]]`. The outer `Chunk` represents batches of requests that must
 * be performed sequentially. The inner `Chunk` represents a batch of requests
 * that can be performed in parallel. This allows data sources to introspect on
 * all the requests being executed and optimize the query.
 *
 * Data sources will typically be parameterized on a subtype of `Request[A]`,
 * though that is not strictly necessarily as long as the data source can map
 * the request type to a `Request[A]`. Data sources can then pattern match on
 * the collection of requests to determine the information requested, execute
 * the query, and place the results into the `CompletedRequestsMap` one of the
 * constructors in [[CompletedRequestMap$]]. Data sources must provide results
 * for all requests received. Failure to do so will cause a query to die with a
 * `QueryFailure` when run.
 */
trait DataSource[-R, -A] { self =>

  /**
   * Syntax for adding aspects.
   */
  final def @@[R1 <: R](aspect: DataSourceAspect[R1]): DataSource[R1, A] =
    aspect(self)

  /**
   * The data source's identifier.
   */
  val identifier: String

  /**
   * Execute a collection of requests. The outer `Chunk` represents batches of
   * requests that must be performed sequentially. The inner `Chunk` represents
   * a batch of requests that can be performed in parallel.
   */
  def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap]

  /**
   * Returns a data source that executes at most `n` requests in parallel.
   */
  def batchN(n: Int): DataSource[R, A] =
    new DataSource[R, A] {
      val identifier = s"${self}.batchN($n)"
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        if (n < 1)
          ZIO.die(new IllegalArgumentException("batchN: n must be at least 1"))
        else
          self.runAll(requests.foldLeft(Chunk.newBuilder[Chunk[A]])(_ addAll _.grouped(n)).result())
    }

  /**
   * Returns a new data source that executes requests of type `B` using the
   * specified function to transform `B` requests into requests that this data
   * source can execute.
   */
  final def contramap[B](f: Described[B => A]): DataSource[R, B] =
    new DataSource[R, B] {
      val identifier = s"${self.identifier}.contramap(${f.description})"
      def runAll(requests: Chunk[Chunk[B]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        self.runAll(requests.map(_.map(f.value)))
    }

  /**
   * Returns a new data source that executes requests of type `B` using the
   * specified effectual function to transform `B` requests into requests that
   * this data source can execute.
   */
  final def contramapZIO[R1 <: R, B](f: Described[B => ZIO[R1, Nothing, A]]): DataSource[R1, B] =
    new DataSource[R1, B] {
      val identifier = s"${self.identifier}.contramapZIO(${f.description})"
      def runAll(requests: Chunk[Chunk[B]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        ZIO.foreach(requests)(ZIO.foreachPar(_)(f.value)).flatMap(self.runAll)
    }

  /**
   * Returns a new data source that executes requests of type `C` using the
   * specified function to transform `C` requests into requests that either this
   * data source or that data source can execute.
   */
  final def eitherWith[R1 <: R, B, C](
    that: DataSource[R1, B]
  )(f: Described[C => Either[A, B]]): DataSource[R1, C] =
    new DataSource[R1, C] {
      val identifier = s"${self.identifier}.eitherWith(${that.identifier})(${f.description})"
      def runAll(requests: Chunk[Chunk[C]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        ZIO.suspendSucceed {
          val iter = requests.iterator
          val crm  = CompletedRequestMap.Mutable.empty(requests.foldLeft(0)(_ + _.size))
          ZIO
            .whileLoop(iter.hasNext) {
              val reqs     = iter.next()
              val (as, bs) = reqs.partitionMap(f.value)
              self.runAll(Chunk.single(as)) <&> that.runAll(Chunk.single(bs))
            } { case (l, r) =>
              crm.addAll(l)
              crm.addAll(r)
            }
            .as(crm)
        }
    }

  override final def equals(that: Any): Boolean =
    that match {
      case that: DataSource[_, _] => this.identifier == that.identifier
      case _                      => false
    }

  override final def hashCode: Int =
    identifier.hashCode

  /**
   * Provides this data source with its required environment.
   */
  final def provideEnvironment(r: Described[ZEnvironment[R]]): DataSource[Any, A] =
    provideSomeEnvironment(Described(_ => r.value, s"_ => ${r.description}"))

  /**
   * Provides this data source with part of its required environment.
   */
  final def provideSomeEnvironment[R0](
    f: Described[ZEnvironment[R0] => ZEnvironment[R]]
  ): DataSource[R0, A] =
    new DataSource[R0, A] {
      val identifier = s"${self.identifier}.provideSomeEnvironment(${f.description})"
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R0, Nothing, CompletedRequestMap] =
        self.runAll(requests).provideSomeEnvironment(f.value)
    }

  /**
   * Returns a new data source that executes requests by sending them to this
   * data source and that data source, returning the results from the first data
   * source to complete and safely interrupting the loser.
   */
  final def race[R1 <: R, A1 <: A](that: DataSource[R1, A1]): DataSource[R1, A1] =
    new DataSource[R1, A1] {
      val identifier = s"${self.identifier}.race(${that.identifier})"
      def runAll(requests: Chunk[Chunk[A1]])(implicit trace: Trace): ZIO[R1, Nothing, CompletedRequestMap] =
        self.runAll(requests).race(that.runAll(requests))
    }

  override final def toString: String =
    identifier
}

object DataSource {
  import UtilsVersionSpecific._

  /**
   * A data source that executes requests that can be performed in parallel in
   * batches but does not further optimize batches of requests that must be
   * performed sequentially.
   */
  trait Batched[-R, -A] extends DataSource[R, A] {
    def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap]

    final def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
      requests.size match {
        case 0 => ZIO.succeed(CompletedRequestMap.empty)
        case 1 =>
          val reqs0 = requests.head
          if (reqs0.nonEmpty) run(reqs0) else ZIO.succeed(CompletedRequestMap.empty)
        case _ =>
          ZIO.suspendSucceed {
            val crm  = CompletedRequestMap.Mutable.empty(requests.foldLeft(0)(_ + _.size))
            val iter = requests.iterator
            ZIO
              .whileLoop(iter.hasNext) {
                val reqs        = iter.next()
                val newRequests = if (crm.isEmpty) reqs else reqs.filterNot(crm.contains)
                ZIO.when(newRequests.nonEmpty)(run(newRequests))
              } {
                case Some(map) => crm.addAll(map)
                case _         => ()
              }
              .as(crm)
          }
      }

  }

  object Batched {

    /**
     * Constructs a data source from a function taking a collection of requests
     * and returning a `CompletedRequestMap`.
     */
    def make[R, A](name: String)(f: Chunk[A] => ZIO[R, Nothing, CompletedRequestMap]): DataSource[R, A] =
      new DataSource.Batched[R, A] {
        val identifier: String = name
        def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
          f(requests)
      }
  }

  /**
   * Constructs a data source from a pure function.
   */
  def fromFunction[A, B](
    name: String
  )(f: A => B)(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    new DataSource.Batched[Any, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] =
        ZIO.succeed(CompletedRequestMap.fromIterableWith(requests)(ev.apply, a => Exit.succeed(f(a))))
    }

  /**
   * Constructs a data source from a pure function that takes a list of requests
   * and returns a list of results of the same size. Each item in the result
   * list must correspond to the item at the same index in the request list.
   */
  def fromFunctionBatched[A, B](
    name: String
  )(f: Chunk[A] => Chunk[B])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionBatchedZIO(name)(as => Exit.succeed(f(as)))

  /**
   * Constructs a data source from a pure function that takes a list of requests
   * and returns a list of optional results of the same size. Each item in the
   * result list must correspond to the item at the same index in the request
   * list.
   */
  def fromFunctionBatchedOption[A, B](
    name: String
  )(f: Chunk[A] => Chunk[Option[B]])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionBatchedOptionZIO(name)(as => Exit.succeed(f(as)))

  /**
   * Constructs a data source from an effectual function that takes a list of
   * requests and returns a list of optional results of the same size. Each item
   * in the result list must correspond to the item at the same index in the
   * request list.
   */
  def fromFunctionBatchedOptionZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[Option[B]]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => requests.map(a => (ev(a), Exit.failCause(e))),
            bs => requests.zipWith(bs)((a, b) => (ev(a), Exit.succeed(b)))
          )
          .map(CompletedRequestMap.fromIterableOption)
    }

  /**
   * Constructs a data source from a function that takes a list of requests and
   * returns a list of results of the same size. Uses the specified function to
   * associate each result with the corresponding effect, allowing the function
   * to return the list of results in a different order than the list of
   * requests.
   */
  def fromFunctionBatchedWith[A, B](
    name: String
  )(f: Chunk[A] => Chunk[B], g: B => Request[Nothing, B])(implicit
    ev: A <:< Request[Nothing, B]
  ): DataSource[Any, A] =
    fromFunctionBatchedWithZIO[Any, Nothing, A, B](name)(as => Exit.succeed(f(as)), g)

  /**
   * Constructs a data source from an effectual function that takes a list of
   * requests and returns a list of results of the same size. Uses the specified
   * function to associate each result with the corresponding effect, allowing
   * the function to return the list of results in a different order than the
   * list of requests.
   */
  def fromFunctionBatchedWithZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[B]], g: B => Request[E, B])(implicit
    ev: A <:< Request[E, B]
  ): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => CompletedRequestMap.failCause(ev.liftCo(requests), e),
            bs => CompletedRequestMap.unsafe.fromWith(bs, bs)(g(_), Exit.succeed)
          )

    }

  /**
   * Constructs a data source from an effectual function that takes a list of
   * requests and returns a list of results of the same size. Each item in the
   * result list must correspond to the item at the same index in the request
   * list.
   */
  def fromFunctionBatchedZIO[R, E, A, B](
    name: String
  )(f: Chunk[A] => ZIO[R, E, Chunk[B]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
          .foldCause(
            e => CompletedRequestMap.failCause(ev.liftCo(requests), e),
            CompletedRequestMap.unsafe.fromSuccesses(ev.liftCo(requests), _)
          )
    }

  /**
   * Constructs a data source from an effectual function.
   */
  def fromFunctionZIO[R, E, A, B](
    name: String
  )(f: A => ZIO[R, E, B])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        ZIO
          .foreachPar(requests)(a => f(a).exit)
          .map(CompletedRequestMap.unsafe.fromExits(ev.liftCo(requests), _))
    }

  /**
   * Constructs a data source from a pure function that may not provide results
   * for all requests received.
   */
  def fromFunctionOption[A, B](
    name: String
  )(f: A => Option[B])(implicit ev: A <:< Request[Nothing, B]): DataSource[Any, A] =
    fromFunctionOptionZIO(name)(a => Exit.succeed(f(a)))

  /**
   * Constructs a data source from an effectual function that may not provide
   * results for all requests received.
   */
  def fromFunctionOptionZIO[R, E, A, B](
    name: String
  )(f: A => ZIO[R, E, Option[B]])(implicit ev: A <:< Request[E, B]): DataSource[R, A] =
    new DataSource.Batched[R, A] {
      val identifier: String = name
      def run(requests: Chunk[A])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        ZIO
          .foreachPar(requests)(a => f(a).exit.map((ev(a), _)))
          .map(CompletedRequestMap.fromIterableOption)
    }

  /**
   * Constructs a data source from a function taking a collection of requests
   * and returning a `CompletedRequestMap`.
   */
  def make[R, A](name: String)(f: Chunk[Chunk[A]] => ZIO[R, Nothing, CompletedRequestMap]): DataSource[R, A] =
    new DataSource[R, A] {
      val identifier: String = name
      def runAll(requests: Chunk[Chunk[A]])(implicit trace: Trace): ZIO[R, Nothing, CompletedRequestMap] =
        f(requests)
    }

  /**
   * A data source that never executes requests.
   */
  val never: DataSource[Any, Any] =
    new DataSource[Any, Any] {
      val identifier = "never"
      def runAll(requests: Chunk[Chunk[Any]])(implicit trace: Trace): ZIO[Any, Nothing, CompletedRequestMap] =
        ZIO.never
    }

}
