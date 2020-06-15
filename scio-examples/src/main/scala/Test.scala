import com.spotify.scio._
import com.spotify.scio.state.State

import scala.collection.JavaConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val sc = ScioContext()

    sc.parallelize(1 to 5)
      .map(i => ("key-%05d".format(i), i))
      .flatMap { case (k, v) => (1 to v).map((k, _)) }
      .mapWithState(State.bag[Int]) { case (k, v, s) =>
        val s0 = s.read().asScala
        s.add(v)
        val s1 = s.read().asScala
        println(s"BAG $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.set[Int]) { case (k, v, s) =>
        val s0 = s.read().asScala
        require(s.addIfAbsent(v).read())
        val s1 = s.read().asScala
        require(s.contains(v).read())
        s.remove(v)
        require(!s.contains(v).read())
        s.add(v)
        println(s"SET $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.value[Int]) { case (k, v, s) =>
        val s0 = s.read()
        val s1 = s0 + v
        s.write(s1)
        println(s"VALUE $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.map[String, Int]) { case (k, v, s) =>
        val s0 = s.entries().read().asScala.map(e => (e.getKey, e.getValue)).toMap
        s.put(k, v)
        val s1 = s.entries().read().asScala.map(e => (e.getKey, e.getValue)).toMap
        println(s"MAP $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.aggregate(Set.empty[Int]).from[Int](_ + _, _ ++ _)) { case (k, v, s) =>
        val s0 = s.read()
        s.add(v)
        val s1 = s.read()
        println(s"AGGREGATE1 $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.fold(0)(_ + _)) { case (k, v, s) =>
        val s0 = s.read()
        s.add(v)
        val s1 = s.read()
        println(s"FOLD1 $k => $v ===== $s0 => $s1")
        (k, v)
      }
      .mapWithState(State.fold[Int]) { case (k, v, s) =>
        val s0 = s.read()
        s.add(v)
        val s1 = s.read()
        println(s"FOLD2 $k => $v ===== $s0 => $s1")
        (k, v)
      }

    sc.run()
  }
}
