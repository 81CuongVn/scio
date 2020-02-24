import com.spotify.scio.bigquery.types.BigQueryType
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.api._

object RefinedTest {

  type Name = String Refined NonEmpty


  @BigQueryType.toTable
  case class Row(name: Name, age: Int)


  def main(args: Array[String]): Unit = {
    println("Trigger Compilation")
  }

}
