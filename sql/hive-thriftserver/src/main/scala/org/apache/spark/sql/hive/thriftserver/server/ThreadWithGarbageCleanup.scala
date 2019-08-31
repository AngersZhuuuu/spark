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

package org.apache.spark.sql.hive.thriftserver.server

import java.util

import org.apache.hadoop.hive.metastore.{HiveMetaStore, RawStore}
import org.apache.spark.internal.Logging


/**
 * A HiveServer2 thread used to construct new server threads.
 * In particular, this thread ensures an orderly cleanup,
 * when killed by its corresponding ExecutorService.
 */
object ThreadWithGarbageCleanup extends Thread {
  def currentThread = Thread.currentThread()
}

class ThreadWithGarbageCleanup(val runnable: Runnable)
  extends Thread(runnable) with Logging {

  val threadRawStoreMap: util.Map[Long, RawStore] = ThreadFactoryWithGarbageCleanup.getThreadRawStoreMap

  /**
   * Add any Thread specific garbage cleanup code here.
   * Currently, it shuts down the RawStore object for this thread if it is not null.
   */
  @throws[Throwable]
  override def finalize(): Unit = {
    cleanRawStore()
    super.finalize()
  }

  private def cleanRawStore(): Unit = {
    val threadId = this.getId
    val threadLocalRawStore = threadRawStoreMap.get(threadId)
    if (threadLocalRawStore != null) {
      logDebug("RawStore: " + threadLocalRawStore + ", for the thread: " + this.getName + " will be closed now.")
      threadLocalRawStore.shutdown()
      threadRawStoreMap.remove(threadId)
    }
  }

  /**
   * Cache the ThreadLocal RawStore object. Called from the corresponding thread.
   */
  def cacheThreadLocalRawStore(): Unit = {
    val threadId = this.getId
    val threadLocalRawStore = HiveMetaStore.HMSHandler.getRawStore
    if (threadLocalRawStore != null && !threadRawStoreMap.containsKey(threadId)) {
      logDebug("Adding RawStore: " + threadLocalRawStore + ", for the thread: " +
        this.getName + " to threadRawStoreMap for future cleanup.")
      threadRawStoreMap.put(threadId, threadLocalRawStore)
    }
  }
}
