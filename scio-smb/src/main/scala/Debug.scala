import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio._
import com.spotify.scio.smb._
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.{JsonSortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.values.TupleTag

import scala.jdk.CollectionConverters._

object Debug {
  def main(args: Array[String]): Unit = {
    val in = args(0)

    val sc = ScioContext()

    sc.sortMergeTransform(
      classOf[String],
      JsonSortedBucketIO
        .read(new TupleTag[TableRow]("ecf1"))
        .from(List(s"$in/ecf1", s"$in/ecf2").asJava),
      JsonSortedBucketIO
        .read(new TupleTag[TableRow]("snapshot"))
        .from(s"$in/snapshot"),
      TargetParallelism.of(2048)
    ).to(
      JsonSortedBucketIO.transformOutput(classOf[String], "key")
        .to(s"$in/output")
        .withFilenamePrefix("part")
    ).via { case (key, (ecfs, snaps), outputCollector) =>
      val ecfList = ecfs.toList
      val snapList = snaps.toList
      require(ecfList.length <= 2)
      require(snapList.length == 1)
      for (e <- ecfList; s <- snapList) {
        val tr = new TableRow()
          .set("key", key)
          .set("ecf", e.get("value"))
          .set("snapshot", s.get("value"))
        outputCollector.accept(tr)
      }
    }

    sc.run()
  }
}

object DebugWrite {
  def main(args: Array[String]): Unit = {
    val out = args(0)

    val ecf1 = (1 to 1000).map { i =>
      new TableRow().set("key", s"user$i").set("value", s"ecf$i")
    }
    val ecf2 = (100 to 1100).map { i =>
      new TableRow().set("key", s"user$i").set("value", s"ecf$i")
    }
    val snapshot = (1 to 1200).map { i =>
      new TableRow().set("key", s"user$i").set("value", s"snapshot$i")
    }

    val sc = ScioContext()

    sc.parallelize(ecf1)
      .saveAsSortedBucket(
        JsonSortedBucketIO
          .write(classOf[String], "key")
          .to(s"$out/ecf1")
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(1024)
          .withNumShards(1)
      )

    sc.parallelize(ecf2)
      .saveAsSortedBucket(
        JsonSortedBucketIO
          .write(classOf[String], "key")
          .to(s"$out/ecf2")
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(1024)
          .withNumShards(1)
      )

    sc.parallelize(snapshot)
      .saveAsSortedBucket(
        JsonSortedBucketIO
          .write(classOf[String], "key")
          .to(s"$out/snapshot")
          .withHashType(HashType.MURMUR3_32)
          .withNumBuckets(1024)
          .withNumShards(1)
      )

    sc.run()
  }
}
