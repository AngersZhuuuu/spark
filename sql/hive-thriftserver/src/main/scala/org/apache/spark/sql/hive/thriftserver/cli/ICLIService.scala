package org.apache.spark.sql.hive.thriftserver.cli

import java.util
import java.util.{List, Map}

import org.apache.spark.sql.hive.thriftserver.auth.ThriftAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus
import org.apache.spark.sql.types.StructType


trait ICLIService {
  @throws[SparkThriftServerSQLException]
  def openSession(username: String, password: String, configuration: util.Map[String, String]): SessionHandle

  @throws[SparkThriftServerSQLException]
  def openSessionWithImpersonation(username: String, password: String, configuration: util.Map[String, String], delegationToken: String): SessionHandle

  @throws[SparkThriftServerSQLException]
  def closeSession(sessionHandle: SessionHandle): Unit

  @throws[SparkThriftServerSQLException]
  def getInfo(sessionHandle: SessionHandle, infoType: GetInfoType): GetInfoValue

  @throws[SparkThriftServerSQLException]
  def executeStatement(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def executeStatementAsync(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getCatalogs(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getSchemas(sessionHandle: SessionHandle, catalogName: String, schemaName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTables(sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, tableTypes: util.List[String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTableTypes(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getColumns(sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, columnName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getFunctions(sessionHandle: SessionHandle, catalogName: String, schemaName: String, functionName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getOperationStatus(opHandle: OperationHandle): OperationStatus

  @throws[SparkThriftServerSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def closeOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def getResultSetMetadata(opHandle: OperationHandle): StructType

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: OperationHandle): RowSet

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: OperationHandle, orientation: FetchOrientation, maxRows: Long, fetchType: FetchType): RowSet

  @throws[SparkThriftServerSQLException]
  def getDelegationToken(sessionHandle: SessionHandle, authFactory: ThriftAuthFactory, owner: String, renewer: String): String

  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(sessionHandle: SessionHandle, authFactory: ThriftAuthFactory, tokenStr: String): Unit

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(sessionHandle: SessionHandle, authFactory: ThriftAuthFactory, tokenStr: String): Unit

}
