package org.apache.spark.sql.hive.thriftserver

import java.util
import java.util.Collections

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.{Service, ServiceException}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class CompositeService(name: String) extends AbstractService(name) with Logging {


  private val serviceList: util.ArrayList[Service] = new util.ArrayList[Service]

  def getServices: util.Collection[Service] = Collections.unmodifiableList(serviceList)

  protected def addService(service: Service): Unit = {
    serviceList.add(service)
  }

  protected def removeService(service: Service): Boolean = serviceList.remove(service)

  override def init(hiveConf: HiveConf): Unit = {
    for (service <- serviceList.asScala) {
      service.init(hiveConf)
    }
    super.init(hiveConf)
  }

  override def start(): Unit = {
    var i = 0
    try {
      val n = serviceList.size
      while (i < n) {
        val service = serviceList.get(i)
        service.start
        i += 1
      }
      super.start
    } catch {
      case e: Throwable =>
        logError("Error starting services " + getName, e)
        // Note that the state of the failed service is still INITED and not
        // STARTED. Even though the last service is not started completely, still
        // call stop() on all services including failed service to make sure cleanup
        // happens.
        stop(i)
        throw new ServiceException("Failed to Start " + getName, e)
    }
  }

  override def stop(): Unit = {
    if (this.getServiceState eq STATE.STOPPED) {
      // The base composite-service is already stopped, don't do anything again.
      return
    }
    if (serviceList.size > 0) {
      stop(serviceList.size - 1)
    }
    super.stop
  }

  private def stop(numOfServicesStarted: Int): Unit = {
    // stop in reserve order of start
    var i = numOfServicesStarted
    while (i >= 0) {
      val service = serviceList.get(i)
      try {
        service.stop
      } catch {
        case t: Throwable =>
          logInfo("Error stopping " + service.getName, t)
      }
      i -= 1
    }
  }

  /**
   * JVM Shutdown hook for CompositeService which will stop the given
   * CompositeService gracefully in case of JVM shutdown.
   */
  class CompositeServiceShutdownHook(val compositeService: CompositeService) extends Runnable {
    override def run(): Unit = {
      try // Stop the Composite Service
      compositeService.stop
      catch {
        case t: Throwable =>
          logInfo("Error stopping " + compositeService.getName, t)
      }
    }
  }

}
