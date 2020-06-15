package fix
package coders

import scalafix.v1._
import scala.meta._

class GenerateCoders extends SemanticRule("GenerateCoders") {
  override def isExperimental = true
  override def isLinter = true
  override def isRewrite = false

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.traverse {
      case term @ Term.Apply(Term.Select(_, Term.Name("map")), _) if term.synthetic.isDefined =>
        println("YOLO ---->")
        println("term      = " + term.syntax)
        println("term struct = " + term.structure)
        println("synthetic = " + term.synthetic)
        println("structure = " + term.synthetic.structure)
        val Some(ApplyTree(_, List(sym))) = term.synthetic
        println("sym = " + sym)
        println("<----\n")

    }
    Patch.empty
  }
}
