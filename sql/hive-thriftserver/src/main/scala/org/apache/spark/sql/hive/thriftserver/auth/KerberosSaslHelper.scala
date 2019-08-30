package org.apache.spark.sql.hive.thriftserver.auth

import java.io.IOException
import java.util
import java.util.Map

import javax.security.sasl.SaslException
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge
import org.apache.hive.service.auth.TSubjectAssumingTransport
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.spark.sql.hive.thriftserver.cli.thrift.ThriftCLIService
import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{TSaslClientTransport, TTransport}

object KerberosSaslHelper {

  def getKerberosProcessorFactory(saslServer: HadoopThriftAuthBridge.Server,
                                  service: ThriftCLIService): KerberosSaslHelper.CLIServiceProcessorFactory = {
    new KerberosSaslHelper.CLIServiceProcessorFactory(saslServer, service)
  }

  @throws[SaslException]
  def getKerberosTransport(principal: String,
                           host: String,
                           underlyingTransport: TTransport,
                           saslProps: util.Map[String, String],
                           assumeSubject: Boolean): TTransport = try {
    val names = principal.split("[/@]")
    if (names.length != 3) {
      throw new IllegalArgumentException("Kerberos principal should have 3 parts: " + principal)
    }
    if (assumeSubject) {
      createSubjectAssumedTransport(principal, underlyingTransport, saslProps)
    } else {
      val authBridge = ShimLoader.getHadoopThriftAuthBridge.createClientWithConf("kerberos")
      authBridge.createClientTransport(principal, host, "KERBEROS", null, underlyingTransport, saslProps)
    }
  } catch {
    case e: IOException =>
      throw new SaslException("Failed to open client transport", e)
  }

  @throws[IOException]
  def createSubjectAssumedTransport(principal: String,
                                    underlyingTransport: TTransport,
                                    saslProps: util.Map[String, String]): TTransport = {
    val names = principal.split("[/@]")
    try {
      val saslTransport =
        new TSaslClientTransport("GSSAPI",
          null,
          names(0),
          names(1),
          saslProps,
          null,
          underlyingTransport)
      new TSubjectAssumingTransport(saslTransport)
    } catch {
      case se: SaslException =>
        throw new IOException("Could not instantiate SASL transport", se)
    }
  }

  @throws[SaslException]
  def getTokenTransport(tokenStr: String,
                        host: String,
                        underlyingTransport: TTransport,
                        saslProps: util.Map[String, String]): TTransport = {
    val authBridge = ShimLoader.getHadoopThriftAuthBridge.createClientWithConf("kerberos")
    try {
      authBridge.createClientTransport(null, host, "DIGEST", tokenStr, underlyingTransport, saslProps)
    } catch {
      case e: IOException =>
        throw new SaslException("Failed to open client transport", e)
    }
  }


  private class CLIServiceProcessorFactory private[auth](val saslServer: HadoopThriftAuthBridge.Server, val service: ThriftCLIService) extends TProcessorFactory(null) {
    override def getProcessor(trans: TTransport): TProcessor = {
      val sqlProcessor = new TCLIService.Processor[TCLIService.Iface](service)
      saslServer.wrapNonAssumingProcessor(sqlProcessor)
    }
  }

}
