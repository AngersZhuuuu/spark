/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.cli.monitor;

import org.apache.spark.status.api.v1.StageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class to render progress bar for Hive on Spark job status.
 * Based on the configuration, appropriate render strategy is selected
 * to show the progress bar on beeline or Hive CLI, as well as for logging
 * the report String.
 */
class RenderStrategy {

  interface UpdateFunction {
    void printStatus(Map<Integer, StageData> progressMap,
                     Map<Integer, StageData> lastProgressMap);
  }

  private abstract static class BaseUpdateFunction implements UpdateFunction {
    protected final SparkSQLCLIMonitor monitor;
    private long lastPrintTime;
    private static final int PRINT_INTERVAL = 3000;
    private final Set<String> completed = new HashSet<String>();
    private String lastReport = null;

    BaseUpdateFunction(SparkSQLCLIMonitor monitor) {
      this.monitor = monitor;
    }

//    private String getReport(Map<Integer, StageData> progressMap) {
//      StringBuilder reportBuffer = new StringBuilder();
//      SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
//      String currentDate = dt.format(new Date());
//      reportBuffer.append(currentDate + "\t");
//
//      // Num of total and completed tasks
//      int sumTotal = 0;
//      int sumComplete = 0;
//
//      SortedSet<Integer> keys = new TreeSet<Integer>(progressMap.keySet());
//      for (Integer stage : keys) {
//        StageData progress = progressMap.get(stage);
//        final int complete = progress.numCompleteTasks();
//        final int total = progress.numTasks();
//        final int running = progress.numActiveTasks();
//        final int failed = progress.numFailedTasks();
//        sumTotal += total;
//        sumComplete += complete;
//        String s = String.valueOf(progress.stageId());
//        String stageName = "Stage-" + s;
//        if (total <= 0) {
//          reportBuffer.append(String.format("%s: -/-\t", stageName));
//        } else {
//          if (complete == total && !completed.contains(s)) {
//            completed.add(s);
//          }
//          if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
//            /* stage is started, but not complete */
//            if (failed > 0) {
//              reportBuffer.append(
//                  String.format(
//                      "%s: %d(+%d,-%d)/%d\t", stageName, complete, running, failed, total));
//            } else {
//              reportBuffer.append(
//                  String.format("%s: %d(+%d)/%d\t", stageName, complete, running, total));
//            }
//          } else {
//            /* stage is waiting for input/slots or complete */
//            if (failed > 0) {
//              /* tasks finished but some failed */
//              reportBuffer.append(
//                  String.format(
//                      "%s: %d(-%d)/%d Finished with failed tasks\t",
//                      stageName, complete, failed, total));
//            } else {
//              if (complete == total) {
//                reportBuffer.append(
//                    String.format("%s: %d/%d Finished\t", stageName, complete, total));
//              } else {
//                reportBuffer.append(String.format("%s: %d/%d\t", stageName, complete, total));
//              }
//            }
//          }
//        }
//      }
//
//      final float progress = (sumTotal == 0) ? 1.0f : (float) sumComplete / (float) sumTotal;
//      monitor.updateProgressedPercentage(progress);
//      return reportBuffer.toString();
//    }

    private boolean isSameAsPreviousProgress(
        Map<Integer, StageData> progressMap,
        Map<Integer, StageData> lastProgressMap) {

      if (lastProgressMap == null) {
        return false;
      }

      if (progressMap.isEmpty()) {
        return lastProgressMap.isEmpty();
      } else {
        if (lastProgressMap.isEmpty()) {
          return false;
        } else {
          if (progressMap.size() != lastProgressMap.size()) {
            return false;
          }
          for (Map.Entry<Integer, StageData> entry : progressMap.entrySet()) {
            if (!lastProgressMap.containsKey(entry.getKey())
                || !progressMap.get(entry.getKey()).equals(lastProgressMap.get(entry.getKey()))) {
              return false;
            }
          }
        }
      }
      return true;
    }


//    private boolean showReport(String report) {
//      return !report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + PRINT_INTERVAL;
//    }

    @Override
    public void printStatus(Map<Integer, StageData> progressMap,
                            Map<Integer, StageData> lastProgressMap) {
      // do not print duplicate status while still in middle of print interval.
      boolean isDuplicateState = isSameAsPreviousProgress(progressMap, lastProgressMap);
      boolean withinInterval = System.currentTimeMillis() <= lastPrintTime + PRINT_INTERVAL;
      if (isDuplicateState && withinInterval) {
        return;
      }

//      String report = getReport(progressMap);
      renderProgress(monitor.getProgressMonitor(progressMap));
//      if (showReport(report)) {
//        renderReport(report);
//        lastReport = report;
//        lastPrintTime = System.currentTimeMillis();
//      }
    }

    /**
     * In this method, we will render progress message in ProgressMonitor(SparkProgressMonitor)
     * This message will be use to show in jdbc InUpdateStream.
     *
     * @param monitor RenderProgress Info message to ProgressMonitor
     */
    abstract void renderProgress(ProgressMonitor monitor);

    /**
     * Generate a progress report after a report interval, this progress message will be show in
     * operation log.
     *
     * @param report
     */
//    abstract void renderReport(String report);
  }

  /**
   * This is used to show progress bar on Beeline while using SparkThriftServer.
   */
  static class LogToFileFunction extends BaseUpdateFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogToFileFunction.class);

    LogToFileFunction(SparkSQLCLIMonitor monitor) {
      super(monitor);
    }

    @Override
    void renderProgress(ProgressMonitor monitor) {
      this.monitor.updateProgressMonitor(monitor);
    }

//    @Override
//    void renderReport(String report) {
//      LOGGER.info(report);
//    }
  }

}


