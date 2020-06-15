/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.state

import java.{util => ju}

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.util.{Functions, NamedDoFn}
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Monoid
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.state.{CombiningState, StateSpec}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.{state => b}

class StatefulSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {
  def mapWithState[S <: b.State, U](state: State[S])(
    f: (K, V, S) => U
  )(implicit kCoder: Coder[K], vCoder: Coder[V], uCoder: Coder[U]): SCollection[U] = {
    self.transform(
      _.withName("TupleToKv").toKV
        .applyTransform(ParDo.of(new StatefulDoFn(self.context, state, f)))
        .setCoder(CoderMaterializer.beam(self.context, uCoder))
    )
  }
}

// FIXME: filter, flatMap, etc.
private class StatefulDoFn[K, V, U, S <: b.State](
                                        context: ScioContext,
                                        state: State[S],
                                        f: (K, V, S) => U
) extends NamedDoFn[KV[K, V], U] {
  @StateId("state") private[this] val spec: b.StateSpec[S] = state.spec(context)

  private[this] val g = ClosureCleaner.clean(f) // defeat closure

  @ProcessElement
  def processElement(ctx: ProcessContext, @StateId("state") s: S): Unit = {
    val kv = ctx.element()
    val u = g(kv.getKey, kv.getValue, s)
    ctx.output(u)
  }
}

sealed trait State[M <: b.State] {
  def spec(context: ScioContext): b.StateSpec[M]
}

object State {
  private def apply[T, S <: b.State](
    f: BCoder[T] => b.StateSpec[S]
  )(implicit coder: Coder[T]): State[S] = new State[S] {
    override def spec(context: ScioContext): b.StateSpec[S] =
      f(CoderMaterializer.beam(context, coder))
  }

  def bag[T: Coder]: State[b.BagState[T]] = State[T, b.BagState[T]](b.StateSpecs.bag(_))
  def set[T: Coder]: State[b.SetState[T]] = State[T, b.SetState[T]](b.StateSpecs.set(_))
  def value[T: Coder]: State[b.ValueState[T]] = State[T, b.ValueState[T]](b.StateSpecs.value(_))

  def map[K, V](implicit koder: Coder[K], voder: Coder[V]): State[b.MapState[K, V]] =
    new State[b.MapState[K, V]] {
      override def spec(context: ScioContext): b.StateSpec[b.MapState[K, V]] =
        b.StateSpecs
          .map(CoderMaterializer.beam(context, koder), CoderMaterializer.beam(context, voder))
    }

  // FIXME: MonoidAggregator?
  // Do we want to support transforms without `zeroValue`?
  // Do we want a Scala wrapper type `S` in user function `f: (K, V, S) => U`?
  def aggregate[U: Coder](zeroValue: => U): MakeAggregate[U] = new MakeAggregate(zeroValue)

  class MakeAggregate[U: Coder](zeroValue: => U) {
    def from[T: Coder](seqOp: (U, T) => U, combOp: (U, U) => U): State[b.CombiningState[T, (U, ju.List[T]), U]] =
    new State[b.CombiningState[T,  (U, ju.List[T]), U]] {
      override def spec(context: ScioContext): StateSpec[CombiningState[T, (U, ju.List[T]), U]] =
        b.StateSpecs.combining(
          CoderMaterializer.beam(context, Coder[(U, ju.List[T])]),
          Functions.aggregateFn(context, zeroValue)(seqOp, combOp)
        )
    }
  }

  def fold[T: Coder](zeroValue: => T)(op: (T, T) => T): State[b.CombiningState[T, (T, ju.List[T]), T]] =
    new State[CombiningState[T, (T, ju.List[T]), T]] {
      override def spec(context: ScioContext): StateSpec[CombiningState[T, (T, ju.List[T]), T]] =
        b.StateSpecs.combining(
          CoderMaterializer.beam(context, Coder[(T, ju.List[T])]),
          Functions.aggregateFn(context, zeroValue)(op, op)
        )
    }

  def fold[T: Coder](implicit mon: Monoid[T]): State[b.CombiningState[T, ju.List[T], T]] =
    new State[CombiningState[T, ju.List[T], T]] {
      override def spec(context: ScioContext): StateSpec[CombiningState[T, ju.List[T], T]] =
        b.StateSpecs.combining(
          CoderMaterializer.beam(context, Coder[ju.List[T]]),
          Functions.reduceFn(context, mon)
        )
    }
}
