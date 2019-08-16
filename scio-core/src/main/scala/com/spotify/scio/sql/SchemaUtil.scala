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

object SchemaUtil {
  def mkCaseClass(schema: Schema, name: String): Seq[Doc] = {
    val prefix = Doc.text("case class ") + Doc.text(name) + Doc.char('(')
    val suffix = Doc.char(')')
    val fields = schema.getFields.asScala.map { f =>
      Doc.text(f.getName) + Doc.char(':') + Doc.space + Doc.text(getType(f.getType))
    }
    val body = Doc.intercalate(Doc.char(',') + Doc.line, fields)
    Seq(body.tightBracketBy(prefix, suffix))
  }

  private def getType(fieldType: FieldType): String = fieldType.getTypeName match {
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
      val e = getType(fieldType.getCollectionElementType)
      s"Array[$e]"
    case TypeName.MAP =>
      val k = getType(fieldType.getMapKeyType)
      val v = getType(fieldType.getMapValueType)
      s"Map[$k, $v]"
    case TypeName.ROW => "XXX" // FIXME
    case TypeName.LOGICAL_TYPE => "XXX" // FIXME
  }

  def main(args: Array[String]): Unit = {
    val schema = Schema.builder()
      .addInt16Field("a")
      .addInt32Field("b")
      .addArrayField("c", FieldType.FLOAT)
      .build()
    println(mkCaseClass(schema, "Schema").head.render(100))
  }
}
