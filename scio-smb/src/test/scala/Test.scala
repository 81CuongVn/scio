/*
 * Copyright 2021 Spotify AB.
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
import com.spotify.scio._
import com.spotify.scio.parquet.types._
import com.spotify.scio.smb._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.smb._
import org.apache.beam.sdk.io.AvroGeneratedUser
import org.apache.beam.sdk.values.TupleTag

object Test {
  def main(args: Array[String]): Unit = {
    writeAvro()
    writeType()
    readAvro()
    readType()
    join()
  }

  def readAvro(): Unit = {
    val sc = ScioContext()
    // Read Avro-SMB-parquet as Scala case classes with projection
    sc.typedParquetFile[AvroProjection]("pq/avro-out/*.parquet")
      .debug()
    sc.run()
  }

  def readType(): Unit = {
    val sc = ScioContext()
    // Read Scala-SMB-parquet as Scala case classes with projection
    sc.typedParquetFile[ScalaProjection]("pq/type-out/*.parquet")
      .debug()
    sc.run()
  }

  def join(): Unit = {
    val sc = ScioContext()

    // SMB join Parquet files written from Avro x case clases, with projections on both
    val lhs = ParquetTypeSortedBucketIO.read(new TupleTag[AvroProjection]("lhs"))
      .from("pq/avro-out")
    val rhs = ParquetTypeSortedBucketIO.read(new TupleTag[ScalaProjection]("rhs"))
      .from("pq/type-out")
    sc.sortMergeJoin(classOf[String], lhs, rhs)
      .debug()

    sc.run()
  }

  def writeAvro(): Unit = {
    val sc = ScioContext()
    val data = (1 to 100).map { i =>
      new AvroGeneratedUser(s"user$i", i, s"color$i")
        .asInstanceOf[GenericRecord]
    }

    // Write Avro specific records as SMB parquet
    sc.parallelize(data)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[CharSequence], "name", AvroGeneratedUser.getClassSchema)
          .withNumBuckets(4)
          .to("pq/avro-out")
      )

    sc.run()
  }

  def writeType(): Unit = {
    val sc = ScioContext()
    val data = (1 to 100).map(i => ScalaUser(s"user$i", i, i.toDouble))

    // Write Scala case classes as SMB parquet
    sc.parallelize(data)
      .saveAsSortedBucket(
        ParquetTypeSortedBucketIO
          .write[String, ScalaUser]("name")
          .withNumBuckets(8)
          .to("pq/type-out")
      )

    sc.run()
  }

  case class ScalaUser(name: String, age: Int, account: Double)

  case class AvroProjection(name: String, favorite_color: Option[String])
  case class ScalaProjection(name: String, age: Int)
}
