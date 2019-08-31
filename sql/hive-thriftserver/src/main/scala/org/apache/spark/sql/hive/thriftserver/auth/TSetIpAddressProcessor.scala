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

package org.apache.spark.sql.hive.thriftserver.auth

import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.hive.service.cli.thrift.TCLIService.Iface
import org.apache.spark.internal.Logging
import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.{TSaslClientTransport, TSaslServerTransport, TSocket, TTransport}

/**
 * This class is responsible for setting the ipAddress for operations executed via HiveServer2.
 *
 * - IP address is only set for operations that calls listeners with hookContext
 * - IP address is only set if the underlying transport mechanism is socket
 *
 * @see org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext
 */
object TSetIpAddressProcessor extends Logging {

  private val THREAD_LOCAL_IP_ADDRESS = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  private val THREAD_LOCAL_USER_NAME = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def getUserIpAddress: String = THREAD_LOCAL_IP_ADDRESS.get

  def getUserName: String = THREAD_LOCAL_USER_NAME.get
}

class TSetIpAddressProcessor[I <: Iface](val iface: Iface)
  extends TCLIService.Processor[Iface](iface)
    with Logging {
  @throws[TException]
  override def process(in: TProtocol, out: TProtocol): Boolean = {
    setIpAddress(in)
    setUserName(in)
    try
      super.process(in, out)
    finally {
      TSetIpAddressProcessor.THREAD_LOCAL_USER_NAME.remove()
      TSetIpAddressProcessor.THREAD_LOCAL_IP_ADDRESS.remove()
    }
  }

  private def setUserName(in: TProtocol): Unit = {
    val transport: TTransport = in.getTransport
    if (transport.isInstanceOf[TSaslServerTransport]) {
      val userName = transport.asInstanceOf[TSaslServerTransport].getSaslServer.getAuthorizationID
      TSetIpAddressProcessor.THREAD_LOCAL_USER_NAME.set(userName)
    }
  }

  protected def setIpAddress(in: TProtocol): Unit = {
    val transport: TTransport = in.getTransport
    val tSocket = getUnderlyingSocketFromTransport(transport)
    if (tSocket == null) {
      logWarning("Unknown Transport, cannot determine ipAddress")
    } else {
      TSetIpAddressProcessor.THREAD_LOCAL_IP_ADDRESS.set(tSocket.getSocket.getInetAddress.getHostAddress)
    }
  }

  private def getUnderlyingSocketFromTransport(transport: TTransport): TSocket = {
    var tp = transport
    while (tp != null) {
      if (transport.isInstanceOf[TSaslServerTransport]) {
        tp = transport.asInstanceOf[TSaslClientTransport].getUnderlyingTransport
      }
      if (transport.isInstanceOf[TSaslClientTransport]) {
        tp = transport.asInstanceOf[TSaslClientTransport].getUnderlyingTransport
      }
      if (transport.isInstanceOf[TSocket]) {
        return tp.asInstanceOf[TSocket]
      }
    }
    null
  }
}

