/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.sql

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.Schema.{FieldType, TypeName}
import org.typelevel.paiges._

import scala.collection.JavaConverters._

// TODO: support CaseFormat
object SchemaUtil {
  def makeSchema(schema: Schema, name: String): Doc = {
    val prefix = Doc.text("object ") + Doc.text(name) + Doc.char('{')
    val suffix = Doc.char('}')
    val body = Doc.intercalate(Doc.line, mkCaseClass(schema, "Schema", ""))
    body.bracketBy(prefix, suffix)
  }

  private def mkCaseClass(schema: Schema, name: String, path: String): Seq[Doc] = {
    val prefix = Doc.text("case class ") + Doc.text(name) + Doc.char('(')
    val suffix = Doc.char(')')
    val nested = Seq.newBuilder[Doc]
    val fields = schema.getFields.asScala.map { f =>
      val (t, n) = getType(f.getType, path + f.getName)
      nested ++= n
      Doc.text(f.getName) + Doc.char(':') + Doc.space + Doc.text(t)
    }
    val body = Doc.intercalate(Doc.char(',') + Doc.line, fields)
    body.tightBracketBy(prefix, suffix) +: nested.result()
  }

  // scalastyle:off cyclomatic.complexity
  private def getType(fieldType: FieldType, path: String): (String, Seq[Doc]) = {
    val nested = Seq.newBuilder[Doc]
    val typeName = fieldType.getTypeName match {
      case TypeName.BYTE => "Byte"
      case TypeName.INT16 => "Short"
      case TypeName.INT32 => "Int"
      case TypeName.INT64 => "Long"
      case TypeName.DECIMAL => "BigDecimal"
      case TypeName.FLOAT => "Float"
      case TypeName.DOUBLE => "Double"
      case TypeName.STRING => "String"
      case TypeName.DATETIME => "org.joda.time.LocalDateTime" // FIXME
      case TypeName.BOOLEAN => "Boolean"
      case TypeName.BYTES => "Array[Byte]"
      case TypeName.ARRAY =>
        val (t, n) = getType(fieldType.getCollectionElementType, path)
        nested ++= n
        s"Array[$t]"
      case TypeName.MAP =>
        val (kt, kn) = getType(fieldType.getMapKeyType, path + "Key")
        val (vt, vn) = getType(fieldType.getMapValueType, path + "Value")
        nested ++= kn
        nested ++= vn
        s"Map[$kt, $vt]"
      case TypeName.ROW =>
        val nestedName = path // FIXME
        nested ++= mkCaseClass(fieldType.getRowSchema, nestedName, path)
        nestedName
      case TypeName.LOGICAL_TYPE => "XXX" // FIXME
    }
    (typeName, nested.result())
  }
  // scalastyle:on cyclomatic.complexity

  def main(args: Array[String]): Unit = {
    val schema = Schema.builder()
      .addInt16Field("i16")
      .addInt32Field("i32")
      .addArrayField("af", FieldType.FLOAT)
      .addDateTimeField("dt")
      .addMapField("m", FieldType.STRING, FieldType.INT64)
      .addRowField("row1", Schema.builder()
        .addInt16Field("i16")
        .addInt32Field("i32")
        .build())
      .addRowField("row2", Schema.builder()
        .addFloatField("f")
        .addDoubleField("d")
        .build())
      .build()

    println(makeSchema(schema, "MySchema").render(100))
  }
}
