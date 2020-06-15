package fix
package coders

import scalafix.v1._
import scala.meta._

class GenerateCoders extends SemanticRule("GenerateCoders") {
  override def isExperimental = true
  override def isLinter = true
  override def isRewrite = false

  private val coderSymbol = Symbol("com/spotify/scio/coders/Coder#")

  private def getCoderParam(s: SymbolInformation): List[SemanticType] = {
    s.signature match {
      case ValueSignature(TypeRef(_, `coderSymbol`, args)) =>
        args
      case _ =>
        Nil
    }
  }

  private def getImplicitCoderParameterList(
    symbol: Symbol
  )(implicit doc: Symtab): List[SemanticType] = {
    symbol.info.get.signature match {
      case MethodSignature(_, parameterLists, _) =>
        parameterLists
          .flatMap(_.filter(_.isImplicit))
          .flatMap(getCoderParam _)
      case _ => ???
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.traverse {
      case term @ Term.Apply(s @ Term.Select(_, Term.Name("map")), _) if term.synthetic.isDefined =>
        println("YOLO ---->")
        println("term      = " + term.syntax)
        println("term struct = " + term.structure)
        println("synthetic = " + term.synthetic)
        println("structure = " + term.synthetic.structure)
        val Some(ApplyTree(_, List(sym))) = term.synthetic
        println("sym = " + sym)
        val coderTypes = getImplicitCoderParameterList(s.synthetic.get.symbol.get)
        println("<----\n")

    }
    Patch.empty
  }
}
