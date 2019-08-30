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

package org.apache.spark.sql.hive.thriftserver.cli.session

import org.apache.hadoop.hive.metastore.IMetaStoreClient

import org.apache.spark.sql.hive.thriftserver.auth.ThriftAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.types.StructType

trait ThriftSession {

  @throws[Exception]
  def open(sessionConfMap: Nothing): Unit

  @throws[SparkThriftServerSQLException]
  def getMetaStoreClient: IMetaStoreClient

  /**
    * getInfo operation handler
    *
    * @param getInfoType
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getInfo(getInfoType: GetInfoType): GetInfoValue

  /**
    * execute operation handler
    *
    * @param statement
    * @param confOverlay
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def executeStatement(statement: String, confOverlay: Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def executeStatementAsync(statement: String, confOverlay: Map[String, String]): OperationHandle

  /**
    * getTypeInfo operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getTypeInfo: OperationHandle

  /**
    * getCatalogs operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getCatalogs: OperationHandle

  /**
    * getSchemas operation handler
    *
    * @param catalogName
    * @param schemaName
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getSchemas(catalogName: String, schemaName: String): OperationHandle

  /**
    * getTables operation handler
    *
    * @param catalogName
    * @param schemaName
    * @param tableName
    * @param tableTypes
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getTables(catalogName: String, schemaName: String, tableName: String, tableTypes: Nothing): OperationHandle

  /**
    * getTableTypes operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getTableTypes: OperationHandle

  /**
    * getColumns operation handler
    *
    * @param catalogName
    * @param schemaName
    * @param tableName
    * @param columnName
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getColumns(catalogName: String, schemaName: String, tableName: String, columnName: String): OperationHandle

  /**
    * getFunctions operation handler
    *
    * @param catalogName
    * @param schemaName
    * @param functionName
    * @return
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def getFunctions(catalogName: String, schemaName: String, functionName: String): OperationHandle

  /**
    * close the session
    *
    * @throws SparkThriftServerSQLException
    */
  @throws[SparkThriftServerSQLException]
  def close(): Unit

  @throws[SparkThriftServerSQLException]
  def cancelOperation(opHandle: Nothing): Unit

  @throws[SparkThriftServerSQLException]
  def closeOperation(opHandle: Nothing): Unit

  @throws[SparkThriftServerSQLException]
  def getResultSetMetadata(opHandle: Nothing): StructType

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: Nothing, orientation: FetchOrientation, maxRows: Long, fetchType: FetchType): RowSet

  @throws[SparkThriftServerSQLException]
  def getDelegationToken(authFactory: ThriftAuthFactory, owner: String, renewer: String): String

  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(authFactory: ThriftAuthFactory, tokenStr: String): Unit

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(authFactory: ThriftAuthFactory, tokenStr: String): Unit

  def closeExpiredOperations(): Unit

  def getNoOperationTime: Long
}
