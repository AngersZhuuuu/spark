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

package org.apache.spark.sql.hive.thriftserver

import java.util.{ArrayList => JArrayList, Arrays, List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.hive.cli.monitor._
import org.apache.spark.sql.hive.cli.monitor.InPlaceUpdateStream.EventNotifier
import org.apache.spark.status.api.v1.StageData


private[hive] class SparkSQLDriver(val context: SQLContext = SparkSQLEnv.sqlContext)
  extends Driver
  with Logging {

  private[hive] var tableSchema: Schema = _
  private[hive] var hiveResponse: Seq[String] = _
  private[hive] var executionId: Option[String] = None
  private[hive] var eventNotifier: EventNotifier = _
  private[hive] var jobStatus: CLISQLStatus = _
  private[hive] var inPlaceUpdateStream: InPlaceUpdateStream = InPlaceUpdateStream.NO_OP
  private[hive] var monitorThread: Option[Thread] = None

  override def init(): Unit = {
    jobStatus = CLISQLStatus.INITING
    eventNotifier = new EventNotifier()
    if (SessionState.get().getIsVerbose) {
      inPlaceUpdateStream = new CLIInPlaceUpdateStream(SessionState.get().out, eventNotifier)
      monitorThread = Some(new Thread(new SparkSQLCLIMonitor(this)))
      monitorThread.foreach(_.start())
    }
  }

  def getExecutionId(): Option[String] = {
    executionId
  }

  def getSQLStatus(): CLISQLStatus = {
    jobStatus
  }

  private def getResultSetSchema(query: QueryExecution): Schema = {
    val analyzed = query.analyzed
    logDebug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.isEmpty) {
      new Schema(Arrays.asList(new FieldSchema("Response code", "string", "")), null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }

      new Schema(fieldSchemas.asJava, null)
    }
  }

  override def run(command: String): CommandProcessorResponse = {
    // TODO unify the error code
    try {
      context.sparkContext.setJobDescription(command)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      jobStatus = CLISQLStatus.COMPILED
      hiveResponse = SQLExecution.withNewExecutionId(context.sparkSession, execution) {
        executionId = Some(context.sparkContext.getLocalProperty(EXECUTION_ID_KEY))
        jobStatus = CLISQLStatus.RUNNING
        hiveResultString(execution.executedPlan)
      }
      tableSchema = getResultSetSchema(execution)
      jobStatus = CLISQLStatus.SUCCEEDED
      new CommandProcessorResponse(0)
    } catch {
        case ae: AnalysisException =>
          logDebug(s"Failed in [$command]", ae)
          jobStatus = CLISQLStatus.FAILED
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(ae), null, ae)
        case cause: Throwable =>
          logError(s"Failed in [$command]", cause)
          jobStatus = CLISQLStatus.FAILED
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(cause), null, cause)
    }
  }

  override def close(): Int = {
    hiveResponse = null
    tableSchema = null
    jobStatus = CLISQLStatus.CLOSED
    try {
      monitorThread.foreach(_.interrupt())
    } catch {
      case e: Throwable =>
        logError("Failed tp interrupt log thread", e)
        if (monitorThread.isDefined && monitorThread.get.isInterrupted) {
        monitorThread.foreach(_.interrupt())
      }
    } finally {
      monitorThread = null
    }
    0
  }

  override def getResults(res: JList[_]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.asInstanceOf[JArrayList[String]].addAll(hiveResponse.asJava)
      hiveResponse = null
      true
    }
  }

  override def getSchema: Schema = tableSchema

  override def destroy(): Unit = {
    super.destroy()
    jobStatus = CLISQLStatus.KILLED
    hiveResponse = null
    tableSchema = null
  }

  def getJobIds(): Seq[Int] = {
    if (executionId.isDefined) {
      val execution = context.sharedState.statusStore.execution(executionId.get.toLong)
      if (execution.isDefined) {
        execution.get.jobs.keys.toSeq
      } else {
        Seq.empty[Int]
      }
    } else {
      Seq.empty[Int]
    }
  }

  def getStatementProgress(): JMap[Integer, StageData] = {
    getJobIds().
      flatMap(id => context.sparkContext.statusStore.job(id).stageIds)
      .map(stageId => stageId -> context.sparkContext.statusStore.stageData(stageId))
      .flatMap(x => {
        x._2.map(data => Integer.valueOf(x._1) -> data)
      }).toMap.asJava
  }

  def updateProgress(progressUpdateMsg: ProgressUpdateMsg): Unit = {
    inPlaceUpdateStream.update(progressUpdateMsg)
  }
}
