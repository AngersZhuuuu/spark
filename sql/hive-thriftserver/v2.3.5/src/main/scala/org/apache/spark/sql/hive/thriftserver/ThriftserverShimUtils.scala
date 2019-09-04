/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.security.AccessController

import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.service.cli.thrift.TProtocolVersion._
import org.apache.spark.service.cli.thrift._
import org.apache.spark.sql.hive.thriftserver.cli.{RowSet, RowSetFactory}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Various utilities for hive-thriftserver used to upgrade the built-in Hive.
 */
private[thriftserver] object ThriftserverShimUtils {

  private[thriftserver] type TProtocolVersion = org.apache.spark.service.cli.thrift.TProtocolVersion
  private[thriftserver] type Client = org.apache.spark.service.cli.thrift.TCLIService.Client
  private[thriftserver] type TOpenSessionReq = org.apache.spark.service.cli.thrift.TOpenSessionReq
  private[thriftserver] type TGetSchemasReq = org.apache.spark.service.cli.thrift.TGetSchemasReq
  private[thriftserver] type TGetTablesReq = org.apache.spark.service.cli.thrift.TGetTablesReq
  private[thriftserver] type TGetColumnsReq = org.apache.spark.service.cli.thrift.TGetColumnsReq
  private[thriftserver] type TGetInfoReq = org.apache.spark.service.cli.thrift.TGetInfoReq
  private[thriftserver] type TExecuteStatementReq =
    org.apache.spark.service.cli.thrift.TExecuteStatementReq

  private[thriftserver] def getConsole: SessionState.LogHelper = {
    val LOG = LoggerFactory.getLogger(classOf[SparkSQLCLIDriver])
    new SessionState.LogHelper(LOG)
  }

  private[thriftserver] def resultRowSet(getResultSetSchema: StructType,
                                         getProtocolVersion: TProtocolVersion): RowSet = {
    RowSetFactory.create(getResultSetSchema, getProtocolVersion)
  }

  private[thriftserver] def toJavaSQLType(s: String): Int = Type.getType(s).toJavaSQLType

  private[thriftserver] def addToClassPath(
                                            loader: ClassLoader,
                                            auxJars: Array[String]): ClassLoader = {
    val addAction = new AddToClassPathAction(loader, auxJars.toList.asJava)
    AccessController.doPrivileged(addAction)
  }

  private[thriftserver] val testedProtocolVersions = Seq(
    HIVE_CLI_SERVICE_PROTOCOL_V1,
    HIVE_CLI_SERVICE_PROTOCOL_V2,
    HIVE_CLI_SERVICE_PROTOCOL_V3,
    HIVE_CLI_SERVICE_PROTOCOL_V4,
    HIVE_CLI_SERVICE_PROTOCOL_V5,
    HIVE_CLI_SERVICE_PROTOCOL_V6,
    HIVE_CLI_SERVICE_PROTOCOL_V7,
    HIVE_CLI_SERVICE_PROTOCOL_V8,
    HIVE_CLI_SERVICE_PROTOCOL_V9,
    HIVE_CLI_SERVICE_PROTOCOL_V10)
}
