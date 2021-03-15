package com.spotify.scio.examples

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.gcs.tools.AvroTools
import com.spotify.scio.gcs.tools.Tools.ProtoTools

// Generate files for gcs-tools integration tests
// https://github.com/spotify/gcs-tools
object GcsToolsTest {
  def main(args: Array[String]): Unit = {
    val sc = ScioContext()
    val avros = sc
      .parallelize(1 to 100)
      .map(i => AvroTools.newBuilder().setId(i).setName(s"user$i").build())
    val protos = sc
      .parallelize(1 to 100)
      .map(i => ProtoTools.newBuilder().setId(i).setName(s"user$i").build())

    avros.saveAsAvroFile("out", numShards = 1)
    avros.saveAsParquetAvroFile("out", numShards = 1)
    protos.saveAsProtobufFile("out", numShards = 1)

    sc.run()
  }
}
