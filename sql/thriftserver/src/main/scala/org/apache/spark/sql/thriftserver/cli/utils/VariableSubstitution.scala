/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.thriftserver.cli.utils

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.{HiveConf, SystemVariables}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.conf.SystemVariables._
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.internal.Logging

private[thriftserver] class VariableSubstitution(hiveVariableSource: util.Map[String, String])
  extends SystemVariables
  with Logging {
  
  override protected def getSubstitute(conf: Configuration, variable: String): String = {
    var value: String = super.getSubstitute(conf, variable)
    if (value == null && SessionState.get != null) {
      if (variable.startsWith(HIVEVAR_PREFIX)) {
        value = hiveVariableSource.get(variable.substring(HIVEVAR_PREFIX.length))
      } else {
        value = hiveVariableSource.get(variable)
      }
    }
    value
  }

  def substitute(conf: HiveConf, expr: String): String = {
    if (expr == null) return expr
    if (HiveConf.getBoolVar(conf, ConfVars.HIVEVARIABLESUBSTITUTE)) {
      logDebug("Substitution is on: " + expr)
    } else {
      return expr
    }
    val depth = HiveConf.getIntVar(conf, ConfVars.HIVEVARIABLESUBSTITUTEDEPTH)
    substitute(conf, expr, depth)
  }
}
