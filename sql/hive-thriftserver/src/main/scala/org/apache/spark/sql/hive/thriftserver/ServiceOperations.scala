package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.Service
import org.apache.spark.internal.Logging

object ServiceOperations extends Logging {

  /**
   * Verify that a service is in a given state.
   *
   * @param state         the actual state a service is in
   * @param expectedState the desired state
   * @throws IllegalStateException if the service state is different from
   *                               the desired state
   */
  def ensureCurrentState(state: Service.STATE, expectedState: Service.STATE): Unit = {
    if (state ne expectedState) {
      throw new IllegalStateException("For this operation, the " +
        "current service state must be " + expectedState + " instead of " + state)
    }
  }

  /**
   * Initialize a service.
   *
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   *
   * @param service       a service that must be in the state
   *                      { @link Service.STATE#NOTINITED}
   * @param configuration the configuration to initialize the service with
   * @throws RuntimeException      on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */
  def init(service: Service, configuration: HiveConf): Unit = {
    val state = service.getServiceState
    ensureCurrentState(state, Service.STATE.NOTINITED)
    service.init(configuration)
  }

  /**
   * Start a service.
   *
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   *
   * @param service a service that must be in the state
   *                { @link Service.STATE#INITED}
   * @throws RuntimeException      on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */
  def start(service: Service): Unit = {
    val state = service.getServiceState
    ensureCurrentState(state, Service.STATE.INITED)
    service.start()
  }

  /**
   * Initialize then start a service.
   *
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   *
   * @param service       a service that must be in the state
   *                      { @link Service.STATE#NOTINITED}
   * @param configuration the configuration to initialize the service with
   * @throws RuntimeException      on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */
  def deploy(service: Service, configuration: HiveConf): Unit = {
    init(service, configuration)
    start(service)
  }

  /**
   * Stop a service.
   *
   * Do nothing if the service is null or not in a state in which it can be/needs to be stopped.
   *
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   *
   * @param service a service or null
   */
  def stop(service: Service): Unit = {
    if (service != null) {
      val state = service.getServiceState
      if (state eq Service.STATE.STARTED) {
        service.stop()

      }
    }

    /**
     * Stop a service; if it is null do nothing. Exceptions are caught and
     * logged at warn level. (but not Throwables). This operation is intended to
     * be used in cleanup operations
     *
     * @param service a service; may be null
     * @return any exception that was caught; null if none was.
     */
    def stopQuietly(service: Service): Exception = {
      try {
        stop(service)
      } catch {
        case e: Exception =>
          logWarning("When stopping the service " + service.getName + " : " + e, e)
          return e
      }
      null
    }
  }
