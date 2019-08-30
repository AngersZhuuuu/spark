package org.apache.spark.sql.hive.thriftserver.server

import org.apache.hive.service.server.ThreadWithGarbageCleanup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadFactory

import org.apache.hadoop.hive.metastore.RawStore

object ThreadFactoryWithGarbageCleanup {

  private val threadRawStoreMap = new ConcurrentHashMap[Long, RawStore]

  def getThreadRawStoreMap: ConcurrentHashMap[Long, RawStore] = threadRawStoreMap
}

class ThreadFactoryWithGarbageCleanup(val namePrefix: String) extends ThreadFactory {
  override def newThread(runnable: Runnable): Thread = {
    val newThread = new ThreadWithGarbageCleanup(runnable)
    newThread.setName(namePrefix + ": Thread-" + newThread.getId)
    newThread
  }
}