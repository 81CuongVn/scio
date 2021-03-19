import com.spotify.scio.testing._
import enumeratum._
import enumeratum.EnumEntry._

sealed abstract class TestType(val value: Int) extends Snakecase

object TestType extends Enum[TestType] {
  val values = findValues
  case object TestTypeObject1 extends TestType(1)
  case object TestTypeObject2 extends TestType(2)
  case object TestTypeObject3 extends TestType(3)
  case object TestTypeObject4 extends TestType(4)
  case object TestTypeObject5 extends TestType(5)
}

case class TestCaseClass(testType: Option[TestType])

class CodersTest extends PipelineSpec {
  import TestType.TestTypeObject2

  val testTypeObject = TestCaseClass(Some(TestTypeObject2))

  "Enumeration encoding for TestType" should "work" in {
    runWithContext { sc =>
      sc.parallelize(Seq(testTypeObject))
    }
  }
}
