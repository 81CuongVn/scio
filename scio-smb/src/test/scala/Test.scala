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

import com.spotify.scio._
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.proto.SimpleV3.SimplePB
import com.spotify.scio.util.ProtobufUtil
import org.apache.avro.file.CodecFactory
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput
import org.apache.beam.sdk.extensions.smb._
import org.apache.beam.sdk.io.{FileSystems, Read}
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._

object Test {
  val metadata1 = new ProtobufBucketMetadata[String, TrackPB](
    16, 1, classOf[String], BucketMetadata.HashType.MURMUR3_32,
    "trackId", "part-")
  val metadata2 = new ProtobufBucketMetadata[String, SimplePB](
    16, 1, classOf[String], BucketMetadata.HashType.MURMUR3_32,
    "trackId", "part-")

  val fileOps1 = ProtobufFileOperation.of(
    classOf[TrackPB],
    CodecFactory.deflateCodec(6),
    ProtobufUtil.schemaMetadataOf[TrackPB].asJava)
  val fileOps2 = ProtobufFileOperation.of(
    classOf[SimplePB],
    CodecFactory.deflateCodec(6),
    ProtobufUtil.schemaMetadataOf[SimplePB].asJava)

  val path1 = FileSystems.matchNewResource("proto-smb1", true)
  val temp1 = FileSystems.matchNewResource("proto-tmp1", true)
  val path2 = FileSystems.matchNewResource("proto-smb2", true)
  val temp2 = FileSystems.matchNewResource("proto-tmp2", true)
  val suffix = ".proto.avro"

  val sink1 = new SortedBucketSink[String, TrackPB](
    metadata1, path1, temp1, suffix, fileOps1, 128)

  val sink2 = new SortedBucketSink[String, SimplePB](
    metadata2, path2, temp2, suffix, fileOps2, 128)

  val tag1 = new TupleTag[TrackPB]()
  val tag2 = new TupleTag[SimplePB]()

  val source = new SortedBucketSource[String](
    classOf[String],
    List(
      new BucketedInput[String, TrackPB](tag1, path1, suffix, fileOps1),
      new BucketedInput[String, SimplePB](tag2, path2, suffix, fileOps2)
    ).asJava.asInstanceOf[java.util.List[BucketedInput[_, _]]]
  )

  def main(args: Array[String]): Unit = {
    val sc = ScioContext()
    // write(sc)
    read(sc)
    sc.run()
  }

  def write(sc: ScioContext): Unit = {
    sc.parallelize(1 to 1000)
      .map(i => TrackPB.newBuilder().setTrackId("track" + i).build())
      .saveAsCustomOutput("1", sink1)

    sc.parallelize(1 to 1000)
      .map(i => SimplePB.newBuilder().setTrackId("track" + i).setPlays(i).build())
      .saveAsCustomOutput("2", sink2)
  }

  def read(sc: ScioContext): Unit = {
    sc.customInput("source", Read.from(source))
      .map { kv =>
        (kv.getKey, kv.getValue.getAll(tag1), kv.getValue.getAll(tag2))
      }
      .debug()
  }
}
