package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.Type
import org.apache.hive.service.cli.thrift.TProtocolVersion._
import org.apache.spark.sql.hive.thriftserver.cli.{RowSet, RowSetFactory}
import org.apache.spark.sql.types.StructType

/**
 * Various utilities for hive-thriftserver used to upgrade the built-in Hive.
 */
private[thriftserver] object ThriftserverShimUtils {

  private[thriftserver] type Client = org.apache.hive.service.cli.thrift.TCLIService.Client
  private[thriftserver] type TOpenSessionReq = org.apache.hive.service.cli.thrift.TOpenSessionReq
  private[thriftserver] type TGetSchemasReq = org.apache.hive.service.cli.thrift.TGetSchemasReq
  private[thriftserver] type TGetTablesReq = org.apache.hive.service.cli.thrift.TGetTablesReq
  private[thriftserver] type TGetColumnsReq = org.apache.hive.service.cli.thrift.TGetColumnsReq
  private[thriftserver] type TGetInfoReq = org.apache.hive.service.cli.thrift.TGetInfoReq
  private[thriftserver] type TExecuteStatementReq =
    org.apache.hive.service.cli.thrift.TExecuteStatementReq

  private[thriftserver] def getConsole: SessionState.LogHelper = {
    val LOG = LogFactory.getLog(classOf[SparkSQLCLIDriver])
    new SessionState.LogHelper(LOG)
  }


  private[thriftserver] def toJavaSQLType(s: String): Int = Type.getType(s).toJavaSQLType

  private[thriftserver] def addToClassPath(loader: ClassLoader,
                                            auxJars: Array[String]): ClassLoader = {
    Utilities.addToClassPath(loader, auxJars)
  }

  private[thriftserver] val testedProtocolVersions = Seq(
    HIVE_CLI_SERVICE_PROTOCOL_V1,
    HIVE_CLI_SERVICE_PROTOCOL_V2,
    HIVE_CLI_SERVICE_PROTOCOL_V3,
    HIVE_CLI_SERVICE_PROTOCOL_V4,
    HIVE_CLI_SERVICE_PROTOCOL_V5,
    HIVE_CLI_SERVICE_PROTOCOL_V6,
    HIVE_CLI_SERVICE_PROTOCOL_V7,
    HIVE_CLI_SERVICE_PROTOCOL_V8)
}
