package com.spotify.scio.bigquery.validation

import com.google.api.services.bigquery.model.TableFieldSchema

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe

class RefinedOverrideTypeProvider extends OverrideTypeProvider {
  override def shouldOverrideType(tfs: TableFieldSchema): Boolean = false

  override def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean = {
    import c.universe._
    tpe match {
      case t@tq"$tpt @$annots" =>
        println(s"type: $tpt")
        true
      case _ =>
        println(s"didn't match: $tpe")
        false
    }
  }

  override def shouldOverrideType(tpe: universe.Type): Boolean = true

  override def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree = ???

  override def createInstance(c: blackbox.Context)(tpe: c.Type, tree: c.Tree): c.Tree = ???

  override def initializeToTable(c: blackbox.Context)(modifiers: c.universe.Modifiers, variableName: c.universe.TermName, tpe: c.universe.Tree): Unit = ???

  override def getBigQueryType(tpe: universe.Type): String = ???
}
