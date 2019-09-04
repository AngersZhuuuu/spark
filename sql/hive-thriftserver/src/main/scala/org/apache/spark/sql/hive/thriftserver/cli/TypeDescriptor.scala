/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.service.cli.thrift.{TPrimitiveTypeEntry, TTypeDesc, TTypeEntry}
import org.apache.spark.sql.types.{DataType, DecimalType}

/**
 * A wrapper class of Spark's [[DataType]] with [[TypeQualifiers]] for [[DecimalType]]s, and could
 * be transformed to [[TTypeDesc]].
 */
case class TypeDescriptor(typ: DataType) {
  private[this] val typeQualifiers: Option[TypeQualifiers] = typ match {
    case d: DecimalType => Some(TypeQualifiers.fromTypeInfo(d))
    case _ => None
  }

  def toTTypeDesc: TTypeDesc = {
    val primitiveEntry = new TPrimitiveTypeEntry(SchemaMapper.toTTypeId(typ))
    typeQualifiers.map(_.toTTypeQualifiers).foreach(primitiveEntry.setTypeQualifiers)
    val entry = TTypeEntry.primitiveEntry(primitiveEntry)
    val desc = new TTypeDesc
    desc.addToTypes(entry)
    desc
  }
}