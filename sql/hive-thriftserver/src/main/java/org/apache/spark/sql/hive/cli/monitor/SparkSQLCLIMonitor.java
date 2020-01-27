package org.apache.spark.sql.hive.cli.monitor;

import org.apache.spark.sql.hive.thriftserver.SparkSQLDriver;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.status.api.v1.StageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * LocalSparkJobMonitor monitor a single Spark job status in a loop until job finished/failed/killed.
 * It print current job status to console and sleep current thread between monitor interval.
 */
public class SparkSQLCLIMonitor implements Runnable {

  private static final String CLASS_NAME = SparkSQLCLIMonitor.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private final long checkInterval;
  private final long monitorTimeoutInterval;
  private final RenderStrategy.UpdateFunction updateFunction;
  protected long startTime;
  private SparkSQLDriver driver;

  protected enum StageState {
    PENDING, RUNNING, FINISHED
  }

  public SparkSQLCLIMonitor(SparkSQLDriver driver) {
    monitorTimeoutInterval = ((long) driver.context().conf().getConf(StaticSQLConf.JOB_MONITOR_TIMEOUT()));
    checkInterval = ((long) driver.context().conf().getConf(StaticSQLConf.JOB_MONITOR_CHECK_INTERVAL()));
    updateFunction = updateFunction();
    this.driver = driver;
  }

  ProgressMonitor getProgressMonitor(Map<Integer, StageData> progressMap) {
    return new SparkProgressMonitor(progressMap, startTime);
  }

  private RenderStrategy.UpdateFunction updateFunction() {
    return new RenderStrategy.LogToFileFunction(this);
  }

//  public void updateProgressedPercentage(float progress) {
//    driver.updateProgressedPercentage(progress);
//  }

  public void updateProgressMonitor(ProgressMonitor monitor) {
    driver.updateProgress(new ProgressUpdateMsg(monitor));
  }

  public void run() {
    boolean running = false;
    boolean done = false;
    Map<Integer, StageData> lastProgressMap = null;

    startTime = System.currentTimeMillis();

    while (true) {
      try {
        CLISQLStatus state = driver.getSQLStatus();

        Map<Integer, StageData> progressMap = driver.getStatementProgress();

        switch (state) {
          case INITING:
            long timeCount = (System.currentTimeMillis() - startTime) / 1000;
            if (timeCount > monitorTimeoutInterval) {
              running = false;
              done = true;
              break;
            }
          case COMPILED:
            if (!running) {
              running = true;
            }
            updateFunction.printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            break;
          case SUCCEEDED:
            updateFunction.printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            running = false;
            done = true;
            break;
          case FAILED:
          case CLOSED:
            running = false;
            done = true;
            break;
          case KILLED:
            running = false;
            done = true;
            break;
        }

        if (!done) {
          Thread.sleep(checkInterval);
        }
      } catch (Exception e) {
        String msg = " with exception '" +
            e.getClass().getName() + "(" + e.getMessage() + ")" + "'";
        msg = "Failed to monitor Job[ " +
            driver.getJobIds().mkString(",") + "]" + msg;

        // Has to use full name to make sure it does not conflict with
        // org.apache.commons.lang.StringUtils
        LOG.error(msg, e);
        done = true;
      } finally {
        if (done) {
          break;
        }
      }
    }
  }
}
