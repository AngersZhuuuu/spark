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

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import org.apache.hive.service.cli.thrift._
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus

import scala.collection.JavaConverters._

class ThriftCLIServiceClient(cliService: TCLIService.Iface) extends CLIServiceClient {

  @throws[SparkThriftServerSQLException]
  def checkStatus(status: TStatus): Unit = {
    if (TStatusCode.ERROR_STATUS == status.getStatusCode) {
      throw new SparkThriftServerSQLException(status)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def openSession(username: String, password: String, configuration: Map[String, String]): SessionHandle = {
    try {
      val req = new TOpenSessionReq
      req.setUsername(username)
      req.setPassword(password)
      req.setConfiguration(configuration.asJava)
      val resp = cliService.OpenSession(req)
      checkStatus(resp.getStatus)
      new SessionHandle(resp.getSessionHandle, resp.getServerProtocolVersion)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def openSessionWithImpersonation(username: String,
                                            password: String,
                                            configuration: Map[String, String],
                                            delegationToken: String) = {
    throw new SparkThriftServerSQLException("open with impersonation operation is not supported in the client")
  }

  @throws[SparkThriftServerSQLException]
  override def closeSession(sessionHandle: SessionHandle): Unit = {
    try {
      val req = new TCloseSessionReq(sessionHandle.toTSessionHandle)
      val resp = cliService.CloseSession(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @throws[SparkThriftServerSQLException]
  override def getInfo(sessionHandle: SessionHandle, infoType: GetInfoType): GetInfoValue = {
    try {
      // FIXME extract the right info type
      val req = new TGetInfoReq(sessionHandle.toTSessionHandle, infoType.toTGetInfoType)
      val resp = cliService.GetInfo(req)
      checkStatus(resp.getStatus)
      new Nothing(resp.getInfoValue)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatement(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def executeStatement(sessionHandle: SessionHandle,
                                statement: String,
                                confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(sessionHandle, statement, confOverlay, false)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatementAsync(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @throws[SparkThriftServerSQLException]
  override def executeStatementAsync(sessionHandle: SessionHandle,
                                     statement: String,
                                     confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(sessionHandle, statement, confOverlay, true)
  }

  @throws[SparkThriftServerSQLException]
  private def executeStatementInternal(sessionHandle: SessionHandle,
                                       statement: String,
                                       confOverlay: Map[String, String], isAsync: Boolean) = {
    try {
      val req = new TExecuteStatementReq(sessionHandle.toTSessionHandle, statement)
      req.setConfOverlay(confOverlay.asJava)
      req.setRunAsync(isAsync)
      val resp = cliService.ExecuteStatement(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTypeInfo(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    try {
      val req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle)
      val resp = cliService.GetTypeInfo(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    try {
      val req = new TGetCatalogsReq(sessionHandle.toTSessionHandle)
      val resp = cliService.GetCatalogs(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @throws[SparkThriftServerSQLException]
  override def getSchemas(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String): OperationHandle = try {
    val req = new TGetSchemasReq(sessionHandle.toTSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = cliService.GetSchemas(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @throws[SparkThriftServerSQLException]
  override def getTables(sessionHandle: SessionHandle,
                         catalogName: String,
                         schemaName: String,
                         tableName: String,
                         tableTypes: List[String]): OperationHandle = try {
    val req = new TGetTablesReq(sessionHandle.toTSessionHandle)
    req.setTableName(tableName)
    req.setTableTypes(tableTypes.asJava)
    req.setSchemaName(schemaName)
    val resp = cliService.GetTables(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = try {
    val req = new TGetTableTypesReq(sessionHandle.toTSessionHandle)
    val resp = cliService.GetTableTypes(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getColumns(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getColumns(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String,
                          tableName: String,
                          columnName: String): OperationHandle = {
    try {
      val req = new TGetColumnsReq
      req.setSessionHandle(sessionHandle.toTSessionHandle)
      req.setCatalogName(catalogName)
      req.setSchemaName(schemaName)
      req.setTableName(tableName)
      req.setColumnName(columnName)
      val resp = cliService.GetColumns(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getFunctions(org.apache.hive.service.cli.SessionHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getFunctions(sessionHandle: SessionHandle, catalogName: String, schemaName: String, functionName: String): OperationHandle = try {
    val req = new TGetFunctionsReq(sessionHandle.toTSessionHandle, functionName)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = cliService.GetFunctions(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = try {
    val req = new TGetOperationStatusReq(opHandle.toTOperationHandle)
    val resp = cliService.GetOperationStatus(req)
    // Checks the status of the RPC call, throws an exception in case of error
    checkStatus(resp.getStatus)
    val opState: OperationState = OperationState.getOperationState(resp.getOperationState)
    var opException: SparkThriftServerSQLException = null
    if (opState == ERROR) {
      opException = new SparkThriftServerSQLException(resp.getErrorMessage, resp.getSqlState, resp.getErrorCode)
    }
    new OperationStatus(opState, opException)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def cancelOperation(opHandle: OperationHandle): Unit = {
    try {
      val req = new TCancelOperationReq(opHandle.toTOperationHandle)
      val resp = cliService.CancelOperation(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def closeOperation(opHandle: OperationHandle): Unit = {
    try {
      val req = new TCloseOperationReq(opHandle.toTOperationHandle)
      val resp = cliService.CloseOperation(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def getResultSetMetadata(opHandle: OperationHandle): Nothing = try {
    val req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle)
    val resp = cliService.GetResultSetMetadata(req)
    checkStatus(resp.getStatus)
    new Nothing(resp.getSchema)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new Nothing(e)
  }

  @throws[SparkThriftServerSQLException]
  override def fetchResults(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long,
                            fetchType: FetchType): RowSet = try {
    val req = new TFetchResultsReq
    req.setOperationHandle(opHandle.toTOperationHandle)
    req.setOrientation(orientation.toTFetchOrientation)
    req.setMaxRows(maxRows)
    req.setFetchType(fetchType.toTFetchType)
    val resp = cliService.FetchResults(req)
    checkStatus(resp.getStatus)
    RowSetFactory.create(resp.getResults, opHandle.getProtocolVersion)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)
   */
  @throws[SparkThriftServerSQLException]
  override def fetchResults(opHandle: OperationHandle): RowSet = {
    // TODO: set the correct default fetch size
    fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10000, FetchType.QUERY_OUTPUT)
  }

  @throws[SparkThriftServerSQLException]
  override def getDelegationToken(sessionHandle: SessionHandle,
                                  authFactory: HiveAuthFactory,
                                  owner: String, renewer: String): String = {
    val req = new TGetDelegationTokenReq(sessionHandle.toTSessionHandle, owner, renewer)
    try {
      val tokenResp = cliService.GetDelegationToken(req)
      checkStatus(tokenResp.getStatus)
      tokenResp.getDelegationToken
    } catch {
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    val cancelReq = new TCancelDelegationTokenReq(sessionHandle.toTSessionHandle, tokenStr)
    try {
      val cancelResp = cliService.CancelDelegationToken(cancelReq)
      checkStatus(cancelResp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    val cancelReq = new TRenewDelegationTokenReq(sessionHandle.toTSessionHandle, tokenStr)
    try {
      val renewResp = cliService.RenewDelegationToken(cancelReq)
      checkStatus(renewResp.getStatus)
    } catch {
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }
}
