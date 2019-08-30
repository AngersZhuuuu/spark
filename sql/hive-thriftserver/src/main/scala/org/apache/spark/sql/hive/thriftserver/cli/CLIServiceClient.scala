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

package org.apache.spark.sql.hive.thriftserver.cli

trait CLIServiceClient extends ICLIService{

  import org.apache.hive.service.auth.HiveAuthFactory
  import org.apache.hive.service.cli.HiveSQLException
  import java.util.Collections

  private val DEFAULT_MAX_ROWS = 1000

  @throws[HiveSQLException]
  def openSession(username: String, password: String): SessionHandle = {
    openSession(username, password, Collections.emptyMap[String, String])
  }

  @throws[HiveSQLException]
  def fetchResults(opHandle: OperationHandle): RowSet = {
    // TODO: provide STATIC default value
    fetchResults(opHandle, FetchOrientation.FETCH_NEXT, DEFAULT_MAX_ROWS, FetchType.QUERY_OUTPUT)
  }

  @throws[HiveSQLException]
  def getDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, owner: String, renewer: String): String

  @throws[HiveSQLException]
  def cancelDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit

  @throws[HiveSQLException]
  def renewDelegationToken(sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit
}
