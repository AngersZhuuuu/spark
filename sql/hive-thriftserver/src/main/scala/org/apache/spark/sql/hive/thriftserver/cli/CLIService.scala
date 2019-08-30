package org.apache.spark.sql.hive.thriftserver.cli

import java.io.IOException
import java.util
import java.util.Map

import javax.security.auth.login.LoginException
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.{CompositeService, ServiceException}
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus
import org.apache.spark.sql.hive.thriftserver.cli.session.SessionManager
import org.apache.spark.sql.types.StructType

class CLIService(hiveServer2: HiveServer2)
  extends CompositeService(classOf[CLIService].getSimpleName)
    with ICLIService {
  private val LOG = LogFactory.getLog(classOf[CLIService].getName)

  private var hiveConf: HiveConf = null
  private var sessionManager: SessionManager = null
  private var serviceUGI: UserGroupInformation = null
  private var httpUGI: UserGroupInformation = null

  override def init(hiveConf: HiveConf): Unit = {
    this.hiveConf = hiveConf
    sessionManager = new SessionManager(hiveServer2)
    addService(sessionManager)
    //  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
    if (UserGroupInformation.isSecurityEnabled) {
      try {
        HiveAuthFactory.loginFromKeytab(hiveConf)
        this.serviceUGI = Utils.getUGI
      } catch {
        case e: IOException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
        case e: LoginException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
      // Also try creating a UGI object for the SPNego principal
      val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL)
      val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB)
      if (principal.isEmpty || keyTabFile.isEmpty) LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal + ", ketabFile: " + keyTabFile)
      else try {
        this.httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
        LOG.info("SPNego httpUGI successfully created.")
      } catch {
        case e: IOException =>
          LOG.warn("SPNego httpUGI creation failed: ", e)
      }
    }
    // creates connection to HMS and thus *must* occur after kerberos login above
    try {
      applyAuthorizationConfigPolicy(hiveConf)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Error applying authorization policy on hive configuration: " + e.getMessage, e)
    }
    setupBlockedUdfs()
    super.init(hiveConf)
  }

  @throws[HiveException]
  @throws[MetaException]
  private def applyAuthorizationConfigPolicy(newHiveConf: HiveConf): Unit = { // authorization setup using SessionState should be revisited eventually, as
    // authorization and authentication are not session specific settings
    val ss = new SessionState(newHiveConf)
    ss.setIsHiveServerQuery(true)
    SessionState.start(ss)
    ss.applyAuthorizationPolicy()
  }

  private def setupBlockedUdfs(): Unit = {
    FunctionRegistry.setupPermissionsForBuiltinUDFs(hiveConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST),
      hiveConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST))
  }

  override def openSession(protocol: TProtocolVersion,
                           username: String,
                           password: String,
                           ipAddress: String,
                           configuration: Predef.Map[String, String]): SessionHandle = {
    val sessionHandle: SessionHandle = sessionManager.openSession(protocol, username, password, ipAddress, configuration, false, null)
    LOG.debug(sessionHandle + ": openSession()")
    return sessionHandle
  }

  override def openSessionWithImpersonation(protocol: TProtocolVersion, username: String, password: String, ipAddress: String, configuration: Predef.Map[String, String], delegationToken: String): SessionHandle = ???

  override def closeSession(sessionHandle: SessionHandle): Unit = ???

  override def getInfo(sessionHandle: SessionHandle, infoType: GetInfoType): GetInfoValue = ???

  override def executeStatement(sessionHandle: SessionHandle, statement: String, confOverlay: Predef.Map[String, String]): OperationHandle = ???

  override def executeStatementAsync(sessionHandle: SessionHandle, statement: String, confOverlay: Predef.Map[String, String]): OperationHandle = ???

  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = ???

  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = ???

  override def getSchemas(sessionHandle: SessionHandle, catalogName: String, schemaName: String): OperationHandle = ???

  override def getTables(sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, tableTypes: List[String]): OperationHandle = ???

  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = ???

  override def getColumns(sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, columnName: String): OperationHandle = ???

  override def getFunctions(sessionHandle: SessionHandle, catalogName: String, schemaName: String, functionName: String): OperationHandle = ???

  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = ???

  override def cancelOperation(opHandle: OperationHandle): Unit = ???

  override def closeOperation(opHandle: OperationHandle): Unit = ???

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = ???

  override def fetchResults(opHandle: OperationHandle): RowSet = ???

  override def fetchResults(opHandle: OperationHandle, orientation: FetchOrientation, maxRows: Long, fetchType: FetchType): RowSet = ???

  // obtain delegation token for the give user from metastore
  @throws[SparkThriftServerSQLException]
  @throws[UnsupportedOperationException]
  @throws[LoginException]
  @throws[IOException]
  def getDelegationTokenFromMetaStore(owner: String): String = {
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL) ||
      !hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      throw new UnsupportedOperationException("delegation token is can only be obtained for a secure remote metastore")
    }
    try {
      Hive.closeCurrent()
      Hive.get(hiveConf).getDelegationToken(owner, owner)
    } catch {
      case e: HiveException =>
        if (e.getCause.isInstanceOf[UnsupportedOperationException]) {
          throw e.getCause.asInstanceOf[UnsupportedOperationException]
        } else {
          throw new SparkThriftServerSQLException("Error connect metastore to setup impersonation", e)
        }
    }
  }

  override def getDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, owner: String, renewer: String): String = ???

  override def cancelDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = ???

  override def renewDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = ???
}

object CLIService {
  val protocols: Array[TProtocolVersion] = TProtocolVersion.values()
  val SERVER_VERSION: TProtocolVersion = protocols(protocols.length - 1)
}