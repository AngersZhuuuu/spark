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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attemptNumber: Int)

private case class CommitOutputSuccess(
    stage: Int,
    stateAttempt: Int,
    partition: Int,
    attemptNumber: Int)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(
    conf: SparkConf,
    isDriver: Boolean,
    sc: Option[SparkContext] = None) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  // Class used to identify a committer. The task ID for a committer is implicitly defined by
  // the partition being processed, but the coordinator needs to keep track of both the stage
  // attempt and the task attempt, because in some situations the same task may be running
  // concurrently in two different attempts of the same stage.
  private case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)

  // Class used to identify a committer 's status when this committer is allowed to commit task.
  // Status is false means this committer is allowed to commit task, status is true means this
  // committer have sent CommitOutputSuccess message to OutputCommitCoordinator.
  private case class CommitStatus(taskIdent: TaskIdentifier, status: Boolean)

  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[CommitStatus](numPartitions)(null)
    val failures = mutable.Map[Int, mutable.Set[TaskIdentifier]]()
  }

  /**
   * Map from active stages's id => authorized task attempts for each partition id, which hold an
   * exclusive lock on committing task output for that partition, as well as any known failed
   * attempts in the stage.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val stageStates = mutable.Map[Int, StageState]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.send()
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by tasks to update commit output success status.
   *
   * If a task attempt has been authorized to commit, after commit task success,
   * task side should update commit status to true.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task can update the commit output success status.
   */
  def commitSuccess(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Unit = {
    val msg = CommitOutputSuccess(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.send(msg)
      case None =>
        logError(
          "commitSuccess called after coordinator was stopped (is SparkEnv shutdown in progress)?")
    }
  }

  /**
   * Called by the DAGScheduler when a stage starts. Initializes the stage's state if it hasn't
   * yet been initialized.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) =>
        require(state.authorizedCommitters.length == maxPartitionId + 1)
        logInfo(s"Reusing state from previous attempt of stage $stage.")

      case _ =>
        stageStates(stage) = new StageState(maxPartitionId + 1)
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    stageStates.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case _: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage.$stageAttempt, " +
          s"partition: $partition, attempt: $attemptNumber")
      case _ =>
        // Mark the attempt as failed to exclude from future commit protocol
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        val commitStatus = stageState.authorizedCommitters(partition)
        if (commitStatus != null && commitStatus.taskIdent == taskId) {
          if (commitStatus.status) {
            sc.foreach(_.dagScheduler.abortStage(stage, s"Authorized committer " +
              s"(attemptNumber=$attemptNumber, stage=$stage, partition=$partition) failed; " +
              s"but task commit success, should fail the job"))
          } else {
            logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
              s"partition=$partition) failed; clearing lock")
            stageState.authorizedCommitters(partition) = null
          }
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        logInfo(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          s"task attempt $attemptNumber already marked as failed.")
        false
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, partition=$partition, " +
            s"task attempt $attemptNumber")
          state.authorizedCommitters(partition) =
            CommitStatus(TaskIdentifier(stageAttempt, attemptNumber), false)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
            s"already committed by ${existing.taskIdent}")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          "stage already marked as completed.")
        false
    }
  }

  private[scheduler] def handleCommitOutputSuccess(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        sc.foreach(_.dagScheduler.abortStage(stage,
          s"Authorized committer (attemptNumber=$attemptNumber, " +
            s"stage=$stage, partition=$partition) failed; but task commit success, " +
            s"should fail the job"))
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          sc.foreach(_.dagScheduler.abortStage(stage,
            s"Authorized committer (attemptNumber=$attemptNumber, " +
              s"stage=$stage, partition=$partition) commit success; partition lock removed " +
              s"before receive CommitOutputSuccess, should fail the job"))
        } else {
          val taskIdent = existing.taskIdent
          if (taskIdent.stageAttempt == stageAttempt && taskIdent.taskAttempt == attemptNumber) {
            logDebug(s"Task Commit success for stage=$stage.$stageAttempt, " +
              s"partition=$partition, task attempt $attemptNumber")
            state.authorizedCommitters(partition) =
              CommitStatus(TaskIdentifier(stageAttempt, attemptNumber), true)
            true
          } else {
            sc.foreach(_.dagScheduler.abortStage(stage,
              s"Authorized committer (attemptNumber=$attemptNumber, " +
              s"stage=$stage, partition=$partition) commit success; but partition lock not " +
              s"consistent, should fail the job"))
          }
        }
      case None =>
        logDebug(s"Commit update status failed for stage=$stage.$stageAttempt, " +
          s"partition=$partition: stage already marked as completed.")
    }
  }

  private def attemptFailed(
      stageState: StageState,
      stageAttempt: Int,
      partition: Int,
      attempt: Int): Boolean = synchronized {
    val failInfo = TaskIdentifier(stageAttempt, attempt)
    stageState.failures.get(partition).exists(_.contains(failInfo))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()

      case CommitOutputSuccess(stage, stageAttempt, partition, attemptNumber) =>
        outputCommitCoordinator.handleCommitOutputSuccess(stage, stageAttempt, partition,
          attemptNumber)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, stageAttempt, partition,
            attemptNumber))
    }
  }
}
