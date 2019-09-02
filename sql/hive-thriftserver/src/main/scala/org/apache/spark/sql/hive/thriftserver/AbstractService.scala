package org.apache.spark.sql.hive.thriftserver

import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.internal.Logging
import org.apache.spark.service.Service.STATE
import org.apache.spark.service.{Service, ServiceStateChangeListener}

/**
 * Construct the service.
 *
 * @param name
 * service name
 */
abstract class AbstractService(val name: String) extends Service with Logging {

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private var state = STATE.NOTINITED
  /**
   * Service start time. Will be zero until the service is started.
   */
  private var startTime = 0L
  /**
   * The configuration. Will be null until the service is initialized.
   */
  private var hiveConf: HiveConf = null
  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  final private val listeners = new util.ArrayList[ServiceStateChangeListener]

  def getServiceState: Service.STATE = synchronized {
    state
  }

  /**
   * {@inheritDoc }
   *
   * @throws IllegalStateException
   * if the current service state does not permit
   * this action
   */
  def init(hiveConf: HiveConf): Unit = {
    ensureCurrentState(STATE.NOTINITED)
    this.hiveConf = hiveConf
    changeState(STATE.INITED)
    logInfo("Service:" + getName + " is inited.")
  }

  def start(): Unit = {
    startTime = System.currentTimeMillis
    ensureCurrentState(STATE.INITED)
    changeState(STATE.STARTED)
    logInfo("Service:" + getName + " is started.")
  }

  def stop(): Unit = {
    if ((state eq STATE.STOPPED) ||
      (state eq STATE.INITED) ||
      (state eq STATE.NOTINITED)) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return
    }
    ensureCurrentState(STATE.STARTED)
    changeState(STATE.STOPPED)
    logInfo("Service:" + getName + " is stopped.")
  }

  def register(l: ServiceStateChangeListener): Unit = {
    listeners.add(l)
  }

  def unregister(l: ServiceStateChangeListener): Unit = {
    listeners.remove(l)
  }

  def getName: String = name

  def getHiveConf: HiveConf = hiveConf

  def getStartTime: Long = startTime

  /**
   * Verify that a service is in a given state.
   *
   * @param currentState
   * the desired state
   * @throws IllegalStateException
   * if the service state is different from
   * the desired state
   */
  private def ensureCurrentState(currentState: Service.STATE): Unit = {
    ServiceOperations.ensureCurrentState(state, currentState)
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods,
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   *
   * @param newState
   * new service state
   */
  private def changeState(newState: Service.STATE): Unit = {
    state = newState
    // notify listeners
    import scala.collection.JavaConversions._
    for (l <- listeners) {
      l.stateChanged(this)
    }
  }
}

