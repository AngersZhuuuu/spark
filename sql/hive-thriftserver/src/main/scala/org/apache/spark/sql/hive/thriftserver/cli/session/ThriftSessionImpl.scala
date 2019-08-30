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

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationManager
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.types.StructType

class ThriftSessionImpl(protocol:TProtocolVersion,
                        username:String,
                        password:String,
                        serverHiveConf:HiveConf,
                        ipAddress:String) extends ThriftSession {

  override def open(sessionConfMap: Map[String, String]): Unit = ???

  override def getMetaStoreClient: IMetaStoreClient = ???

  /**
    * getInfo operation handler
    *
    * @param getInfoType
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getInfo(getInfoType: GetInfoType): GetInfoValue = ???

  /**
    * execute operation handler
    *
    * @param statement
    * @param confOverlay
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def executeStatement(statement: String, confOverlay: Map[String, String]): OperationHandle = ???

  override def executeStatementAsync(statement: String, confOverlay: Map[String, String]): OperationHandle = ???

  /**
    * getTypeInfo operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getTypeInfo: OperationHandle = ???

  /**
    * getCatalogs operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getCatalogs: OperationHandle = ???

  /**
    * getSchemas operation handler
    *
    * @param catalogName
    * @param schemaName
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getSchemas(catalogName: String, schemaName: String): OperationHandle = ???

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
  override def getTables(catalogName: String, schemaName: String, tableName: String, tableTypes: Seq[String]): OperationHandle = ???

  /**
    * getTableTypes operation handler
    *
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getTableTypes: OperationHandle = ???

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
  override def getColumns(catalogName: String, schemaName: String, tableName: String, columnName: String): OperationHandle = ???

  /**
    * getFunctions operation handler
    *
    * @param catalogName
    * @param schemaName
    * @param functionName
    * @return
    * @throws SparkThriftServerSQLException
    */
  override def getFunctions(catalogName: String, schemaName: String, functionName: String): OperationHandle = ???

  /**
    * close the session
    *
    * @throws SparkThriftServerSQLException
    */
  override def close(): Unit = ???

  override def cancelOperation(opHandle: OperationHandle): Unit = ???

  override def closeOperation(opHandle: OperationHandle): Unit = ???

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = ???

  override def fetchResults(opHandle: OperationHandle, orientation: FetchOrientation, maxRows: Long, fetchType: FetchType): RowSet = ???

  override def getDelegationToken(authFactory: HiveAuthFactory, owner: String, renewer: String): String = ???

  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = ???

  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = ???

  override def closeExpiredOperations(): Unit = ???

  override def getNoOperationTime: Long = ???

  override def getProtocolVersion: TProtocolVersion = ???

  /**
    * Set the session manager for the session
    *
    * @param sessionManager
    */
  override def setSessionManager(sessionManager: SessionManager): Unit = ???

  /**
    * Get the session manager for the session
    */
  override def getSessionManager: SessionManager = ???

  /**
    * Set operation manager for the session
    *
    * @param operationManager
    */
  override def setOperationManager(operationManager: OperationManager): Unit = ???

  /**
    * Check whether operation logging is enabled and session dir is created successfully
    */
  override def isOperationLogEnabled: Boolean = ???

  /**
    * Get the session dir, which is the parent dir of operation logs
    *
    * @return a file representing the parent directory of operation logs
    */
  override def getOperationLogSessionDir: File = ???

  /**
    * Set the session dir, which is the parent dir of operation logs
    *
    * @param operationLogRootDir the parent dir of the session dir
    */
  override def setOperationLogSessionDir(operationLogRootDir: File): Unit = ???

  override def getSessionHandle: SessionHandle = ???

  override def getUsername: String = ???

  override def getPassword: String = ???

  override def getHiveConf: HiveConf = ???

  override def getSessionState: SessionState = ???

  override def getUserName: String = ???

  override def setUserName(userName: String): Unit = ???

  override def getIpAddress: String = ???

  override def setIpAddress(ipAddress: String): Unit = ???

  override def getLastAccessTime: Long = ???
}
