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

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus
import org.apache.spark.sql.types.StructType

class EmbeddedCLIServiceClient(cliService: ICLIService) extends CLIServiceClient {

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def openSession(username: String,
                           password: String,
                           configuration: Map[String, String]): SessionHandle = {
    cliService.openSession(username, password, configuration)
  }

  @throws[SparkThriftServerSQLException]
  override def openSessionWithImpersonation(username: String,
                                            password: String,
                                            configuration: Map[String, String],
                                            delegationToken: String): SessionHandle = {
    throw new Nothing("Impersonated session is not supported in the embedded mode")
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def closeSession(sessionHandle: SessionHandle): Unit = {
    cliService.closeSession(sessionHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @throws[SparkThriftServerSQLException]
  override def getInfo(sessionHandle: SessionHandle,
                       getInfoType: GetInfoType): GetInfoValue = {
    cliService.getInfo(sessionHandle, getInfoType)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#executeStatement(org.apache.hive.service.cli.SessionHandle,
   *  java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def executeStatement(sessionHandle: SessionHandle,
                                statement: String,
                                confOverlay: Map[String, String]): OperationHandle = {
    cliService.executeStatement(sessionHandle, statement, confOverlay)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#executeStatementAsync(org.apache.hive.service.cli.SessionHandle,
   *  java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def executeStatementAsync(sessionHandle: SessionHandle,
                                     statement: String,
                                     confOverlay: Map[String, String]): OperationHandle = {
    cliService.executeStatementAsync(sessionHandle, statement, confOverlay)
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getTypeInfo(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    cliService.getTypeInfo(sessionHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */ @throws[SparkThriftServerSQLException]
  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    cliService.getCatalogs(sessionHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @throws[SparkThriftServerSQLException]
  override def getSchemas(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String): OperationHandle = {
    cliService.getSchemas(sessionHandle, catalogName, schemaName)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @throws[SparkThriftServerSQLException]
  override def getTables(sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, tableTypes: List[String]): OperationHandle = {
    cliService.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    cliService.getTableTypes(sessionHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getColumns(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @throws[SparkThriftServerSQLException]
  override def getColumns(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String,
                          tableName: String,
                          columnName: String): OperationHandle = {
    cliService.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getFunctions(org.apache.hive.service.cli.SessionHandle, java.lang.String)
   */
  @throws[SparkThriftServerSQLException]
  override def getFunctions(sessionHandle: SessionHandle,
                            catalogName: String,
                            schemaName: String,
                            functionName: String): OperationHandle = {
    cliService.getFunctions(sessionHandle, catalogName, schemaName, functionName)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    cliService.getOperationStatus(opHandle)
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def cancelOperation(opHandle: OperationHandle): Unit = {
    cliService.cancelOperation(opHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def closeOperation(opHandle: OperationHandle): Unit = {
    cliService.closeOperation(opHandle)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceClient#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    cliService.getResultSetMetadata(opHandle)
  }


  @throws[SparkThriftServerSQLException]
  override def fetchResults(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long,
                            fetchType: FetchType): RowSet = {
    cliService.fetchResults(opHandle, orientation, maxRows, fetchType)
  }

  @throws[SparkThriftServerSQLException]
  override def getDelegationToken(sessionHandle: SessionHandle,
                                  authFactory: HiveAuthFactory,
                                  owner: String,
                                  renewer: String): String = {
    cliService.getDelegationToken(sessionHandle, authFactory, owner, renewer)
  }

  @throws[SparkThriftServerSQLException]
  override def cancelDelegationToken(sessionHandle: SessionHandle,
                                     authFactory: HiveAuthFactory,
                                     tokenStr: String): Unit = {
    cliService.cancelDelegationToken(sessionHandle, authFactory, tokenStr)
  }

  @throws[SparkThriftServerSQLException]
  override def renewDelegationToken(sessionHandle: SessionHandle,
                                    authFactory: HiveAuthFactory,
                                    tokenStr: String): Unit = {
    cliService.renewDelegationToken(sessionHandle, authFactory, tokenStr)
  }

}
