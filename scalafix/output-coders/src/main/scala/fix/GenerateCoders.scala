package fix
package coders

import com.google.protobuf.Message
import com.spotify.scio._
import com.spotify.scio.avro._
import scala.reflect.ClassTag
import com.spotify.scio.values.SCollection

case class Foo(n: String, i: Int)
case class Bar(f: Foo)

object AddMissingImports {

  def computeAndSaveDay[M <: Message : ClassTag](sc: ScioContext): Unit = {
    sc.protobufFile[M]("input")
      .map(m => Foo("hello", 1))
      .flatMap { f =>
        Option(Bar(f))
      }
      .saveAsTextFile("output")
    sc.run()
    ()
  }
}
