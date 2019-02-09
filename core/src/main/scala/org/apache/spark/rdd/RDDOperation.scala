/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object RDDOperation{
  lazy val sparkSession = new SparkSession(SparkContext.getActive.get)
  import sparkSession.implicits._

  def rdd2dataset: Boolean = sparkSession.sessionState.conf.rdd2dataset

  def range(rdd: RDD[Long],
            start: Long, end: Long, step: Long): RDDOperation[Long] = {
    val ds = if (rdd2dataset) sparkSession.range(start, end, step).as[Long]
             else null
    RDDOperation(s"SparkContext.range($start, $end, $step)", rdd, ds, None)
  }

  def textFile(rdd: RDD[String], path: String): RDDOperation[String] = {
    val ds = if (rdd2dataset) sparkSession.read.textFile(path) else null
    RDDOperation(s"SparkContext.textFile($path)", rdd, ds, None)
  }

  def parallelize[T: TypeTag](rdd: RDD[T], seq: Seq[T]): RDDOperation[T] = {
    implicit val enc: Encoder[T] = ExpressionEncoder[T]
    seq match {
      case r: Range =>
        val ds = if (r.isInclusive) {
          sparkSession.range(r.start, r.end + 1, r.step)
        } else {
          sparkSession.range(r.start, r.end, r.step)
        }
        RDDOperation(s"parallelize(range)", rdd, ds.map(_.toInt).as[T], None)
      case _ =>
        val ds = sparkSession.createDataset(seq)
        RDDOperation(s"parallelize", rdd, ds, None)
    }
  }
}


case class RDDOperation[T](val name: String,
                           val rdd: RDD[T],
                           val dataset: Dataset[T],
                           val parent: Option[RDDOperation[_]]) {
  import RDDOperation._
  import sparkSession.implicits._

  def cache(): Unit = if (rdd2dataset) dataset.cache()

  def persist(newLevel: StorageLevel): Unit = if (rdd2dataset) dataset.persist(newLevel)

  def count(): Long = {
    // logWarning("Transforming to Dataset operations")
    if (rdd2dataset) dataset.count()
    else rdd._count()
  }

  def saveAsTextFile(path: String): Unit = {
    if (rdd2dataset) dataset.map(_.toString).write.text(path)
    else rdd._saveAsTextFile(path)
  }

  def reduce(f: (T, T) => T) (implicit ttag: TypeTag[T]): T = {
    if (rdd2dataset) {
      // complete version
      // val udaf = new Aggregator[T, (T, Boolean), T] {
      //   def zero: (T, Boolean) = (0.asInstanceOf[T], false)
      //   def reduce(b: (T, Boolean), a: T): (T, Boolean) =
      //     if (b._2) (f(a, b._1), true) else (a, true)
      //   def merge(b1: (T, Boolean), b2: (T, Boolean)): (T, Boolean) =
      //     if (b1._2 && b2._2) (f(b1._1, b2._1), true)
      //     else if (b1._2) b1
      //     else if (b2._2) b2
      //     else b1
      //   def finish(r: (T, Boolean)): T = if (r._2) r._1 else throw new Exception()
      //   def bufferEncoder: Encoder[(T, Boolean)] = ExpressionEncoder[(T, Boolean)]
      //   def outputEncoder: Encoder[T] = ExpressionEncoder[T]
      // }.toColumn

      // simplified version
      val udaf = new Aggregator[T, T, T] {
        def zero: T = 0.asInstanceOf[T]
        def reduce(b: T, a: T): T = f(a, b)
        def merge(b1: T, b2: T): T = f(b1, b2)
        def finish(r: T): T = r
        def bufferEncoder: Encoder[T] = ExpressionEncoder[T]
        def outputEncoder: Encoder[T] = ExpressionEncoder[T]
      }.toColumn

      // Long
      // val udaf = new Aggregator[Long, Long, Long] {
      //   def zero: Long = 0.asInstanceOf[Long]
      //   def reduce(b: Long, a: Long): Long = f.asInstanceOf[(Long, Long) => Long](a, b)
      //   def merge(b1: Long, b2: Long): Long = f.asInstanceOf[(Long, Long) => Long](b1, b2)
      //   def finish(r: Long): Long = r
      //   def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]
      //   def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]
      // }.asInstanceOf[Aggregator[T, T, T]].toColumn

      dataset.select(udaf).first
    }
    else rdd._reduce(f)
  }

  def map[U: TypeTag](rdd: RDD[U], f: T => U): RDDOperation[U] = {
    val ds = if (rdd2dataset) dataset.map(f)(ExpressionEncoder[U]())
             else null
    RDDOperation(s"map", rdd, ds, Some(this))
  }

  def flatMap[U: TypeTag](rdd: RDD[U], f: T => TraversableOnce[U]): RDDOperation[U] = {
    val ds = if (rdd2dataset) dataset.flatMap(f)(ExpressionEncoder[U]())
             else null
    RDDOperation(s"flatMap", rdd, ds, Some(this))
  }

  def filter(rdd: RDD[T], f: T => Boolean): RDDOperation[T] = {
    val ds = if (rdd2dataset) dataset.filter(f) else null
    RDDOperation(s"filter", rdd, ds, Some(this))
  }

  def distinct(rdd: RDD[T]): RDDOperation[T] = {
    val ds = if (rdd2dataset) dataset.distinct() else null
    RDDOperation(s"distinct", rdd, ds, Some(this))
  }
}

case class PairRDDOperation[K, V](self: RDDOperation[(K, V)]) {
  import RDDOperation._
  import sparkSession.sqlContext.implicits._

  def reduceByKey(rdd: RDD[(K, V)], func: (V, V) => V)
  (implicit ktt: TypeTag[K], vtt: TypeTag[V]): RDDOperation[(K, V)] = {
    val ds = if (rdd2dataset) {
      val vEncoder = ExpressionEncoder[V].resolveAndBind()
      lazy val serializer = GenerateSafeProjection.generate(vEncoder.serializer)
      lazy val tmpRow = new GenericInternalRow(1)

      val udaf = new UserDefinedAggregateFunction {
        override def inputSchema: StructType = vEncoder.schema

        override def bufferSchema: StructType = vEncoder.schema

        override def dataType: DataType = vEncoder.schema(0).dataType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit =
          for (i <- 0 until buffer.size) buffer(i) = null

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
          val a = vEncoder.fromRow(InternalRow(buffer.toSeq: _*))
          val b = vEncoder.fromRow(InternalRow(input.toSeq: _*))
          tmpRow(0) = func(a, b)
          val row = serializer(tmpRow).asInstanceOf[SpecificInternalRow].copy.values
          for (i <- 0 until row.size) buffer(i) = row(i)
        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
          val a = vEncoder.fromRow(InternalRow(buffer1.toSeq: _*))
          val b = vEncoder.fromRow(InternalRow(buffer2.toSeq: _*))
          tmpRow(0) = func(a, b)
          val row = serializer(tmpRow).asInstanceOf[SpecificInternalRow].copy.values
          for (i <- 0 until row.size) buffer1(i) = row(i)
        }

        override def evaluate(buffer: Row): Any = {
          vEncoder.fromRow(InternalRow(buffer.toSeq: _*))
        }
      }

      sparkSession.udf.register("udaf", udaf)
      self.dataset.groupBy($"_1").agg(("_2", "udaf")).as[(K, V)](ExpressionEncoder[(K, V)])
    } else null
    RDDOperation(s"reduceByKey", rdd, ds, Some(self))
  }

  def groupByKey(rdd: RDD[(K, Iterable[V])])
  (implicit ktag: TypeTag[K], vtag: TypeTag[V]): RDDOperation[(K, Iterable[V])] = {
    val ds = if (rdd2dataset) {
      self.dataset.groupBy($"_1")
          .agg(collect_list($"_2").alias("_2"))
          .as[(K, Seq[V])](ExpressionEncoder[(K, Seq[V])])
    } else null
    RDDOperation(s"groupByKey", rdd, ds.asInstanceOf[Dataset[(K, Iterable[V])]], Some(self))
  }

  def mapValues[U](rdd: RDD[(K, U)], f: V => U)
  (implicit ktag: TypeTag[K], utag: TypeTag[U]): RDDOperation[(K, U)] = {
    val ds = if (rdd2dataset) self.dataset.map(kv => (kv._1, f(kv._2)))(ExpressionEncoder[(K, U)])
             else null
    RDDOperation(s"mapValues", rdd, ds, Some(self))
  }

  def join[W](rdd: RDD[(K, (V, W))], other: RDD[(K, W)])
  (implicit kt: TypeTag[K], vt: TypeTag[V], wt: TypeTag[W]): RDDOperation[(K, (V, W))] = {
    val ds = if (rdd2dataset) {
      val ds1 = self.dataset
      val ds2 = other.operation.get.dataset
      ds1.joinWith(ds2, ds1("_1")===ds2("_1"), "inner")
         .select($"_1._1", struct($"_1._2", $"_2._2"))
         .as[(K, (V, W))](ExpressionEncoder[(K, (V, W))])
    } else null
    RDDOperation(s"join", rdd, ds, Some(self))
  }

  def values(rdd: RDD[V])(implicit vt: TypeTag[V]): RDDOperation[V] = {
    val ds = if (rdd2dataset) self.dataset.select($"_2").as[V](ExpressionEncoder[V]) else null
    RDDOperation(s"values", rdd, ds, Some(self))
  }
}

case class OrderedRDDOperation[K, V, P <: Product2[K, V]](self: RDDOperation[P]) {
  import RDDOperation._
  import sparkSession.sqlContext.implicits._

  def sortByKey(rdd: RDD[(K, V)], ascending: Boolean)
  (implicit ktag: TypeTag[K], vtag: TypeTag[V]): RDDOperation[(K, V)] = {
    val ds = if (rdd2dataset) self.dataset.as[(K, V)].sort(if (ascending) $"_1".asc else $"_1".desc)
             else null
    RDDOperation(s"sortByKey", rdd, ds, Some(self))
  }
}
