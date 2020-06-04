package fix
package coders

import scalafix.v1._
import scala.meta._

class GenerateCoders extends SemanticRule("GenerateCoders") {
  override def isExperimental = true
  override def isLinter = false
  override def isRewrite = false

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.traverse {
      // case term: Term if term.synthetic.isDefined =>
      //   println("term      = " + term.syntax)
      //   println("synthetic = " + term.synthetic)
      //   println("structure = " + term.synthetic.structure)
      case t @ Term.Apply(Term.Select(_, Term.Name("map")), _) =>
        println("---->")
        println("term      = " + t.syntax)
        // println("structure = " + t.structure)
        println("synthetic = " + t.synthetic)
        println("structure = " + t.synthetic.structure)
    }
    Patch.empty
  }
}
