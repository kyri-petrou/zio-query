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

import zio._
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.ConcurrentHashMap

/**
 * A `Cache` maintains an internal state with a mapping from requests to
 * `Promise`s that will contain the result of those requests when they are
 * executed. This is used internally by the library to provide deduplication and
 * caching of requests.
 */
abstract class Cache {

  /**
   * Looks up a request in the cache, failing with the unit value if the request
   * is not in the cache or succeeding with a `Promise` if the request is in the
   * cache that will contain the result of the request when it is executed.
   */
  def get[E, A, B](
    datasource: DataSource[?, A],
    request: A
  )(implicit
    trace: Trace,
    ev: A <:< Request[E, B]
  ): IO[Unit, Promise[E, B]]

  /**
   * Looks up a request in the cache. If the request is not in the cache returns
   * a `Left` with a `Promise` that can be completed to complete the request. If
   * the request is in the cache returns a `Right` with a `Promise` that will
   * contain the result of the request when it is executed.
   */
  def lookup[E, A, B](
    dataSource: DataSource[?, A],
    request: A
  )(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Either[Promise[E, B], Promise[E, B]]]

  /**
   * Inserts a request and a `Promise` that will contain the result of the
   * request when it is executed into the cache.
   */
  def put[E, A, B](
    dataSource: DataSource[?, A],
    request: A,
    result: Promise[E, B]
  )(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Unit]

  /**
   * Removes a request from the cache.
   */
  def remove[E, A, B](
    dataSource: DataSource[?, A],
    request: A
  )(implicit
    ev: A <:< Request[E, B],
    trace: Trace
  ): UIO[Unit]
}

object Cache {
  /**
   * Constructs an empty cache.
   */
  def empty(implicit trace: Trace): UIO[Cache] =
    ZIO.succeed(Cache.unsafeMake())

  /**
   * Constructs an empty cache, sized to accommodate the specified number of
   * elements without the need for the internal data structures to be resized.
   */
  def empty(expectedNumOfElements: Int)(implicit trace: Trace): UIO[Cache] =
    ZIO.succeed(Cache.unsafeMake(expectedNumOfElements))

  private type Key = (DataSource[_, _], Request[_, _])

  private[query] final class Default(private val map: ConcurrentHashMap[Key, Promise[_, _]]) extends Cache {

    def get[E, A, B](ds: DataSource[?, A], request: A)(implicit
      trace: Trace,
      ev: A <:< Request[E, B]
    ): IO[Unit, Promise[E, B]] =
      ZIO.suspendSucceed {
        val out = map.get((ds, request)).asInstanceOf[Promise[E, B]]
        if (out eq null) Exit.fail(()) else Exit.succeed(out)
      }

    def lookup[E, A, B](ds: DataSource[?, A], request: A)(implicit
      ev: A <:< Request[E, B],
      trace: Trace
    ): UIO[Either[Promise[E, B], Promise[E, B]]] =
      ZIO.succeed(lookupUnsafe(ds, request)(ev, Unsafe.unsafe))

    def lookupUnsafe[E, A, B](ds: DataSource[?, A], request: A)(implicit
      ev: A <:< Request[E, B],
      unsafe: Unsafe
    ): Either[Promise[E, B], Promise[E, B]] = {
      val newPromise = Promise.unsafe.make[E, B](FiberId.None)
      val existing   = map.putIfAbsent((ds, request), newPromise).asInstanceOf[Promise[E, B]]
      if (existing eq null) Left(newPromise) else Right(existing)
    }

    def put[E, A, B](ds: DataSource[?, A], request: A, result: Promise[E, B])(implicit
      ev: A <:< Request[E, B],
      trace: Trace
    ): UIO[Unit] =
      ZIO.succeed(map.put((ds, request), result))

    def remove[E, A, B](ds: DataSource[?, A], request: A)(implicit
      ev: A <:< Request[E, B],
      trace: Trace
    ): UIO[Unit] =
      ZIO.succeed(map.remove((ds, request)))
  }

  // TODO: Initialize the map with a sensible default value. Default is 16, which seems way too small for a cache
  private[query] def unsafeMake(): Cache = new Default(new ConcurrentHashMap())

  private[query] def unsafeMake(expectedNumOfElements: Int): Cache = {
    val initialSize = Math.ceil(expectedNumOfElements / 0.75d).toInt
    new Default(new ConcurrentHashMap(initialSize))
  }
}
