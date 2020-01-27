package org.apache.spark.sql.hive.cli.monitor;


import org.apache.spark.status.api.v1.StageData;

import java.util.*;

/**
 * This class defines various parts of the progress update bar.
 * Progressbar is displayed in hive-cli and typically rendered using InPlaceUpdate.
 */
class SparkProgressMonitor implements ProgressMonitor {

  private Map<Integer, StageData> progressMap;
  private long startTime;
  private static final int COLUMN_1_WIDTH = 16;

  SparkProgressMonitor(Map<Integer, StageData> progressMap, long startTime) {
    this.progressMap = progressMap;
    this.startTime = startTime;
  }

  @Override
  public List<String> headers() {
    return Arrays.asList("STAGES", "ATTEMPT", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "");
  }

  @Override
  public List<List<String>> rows() {
    List<List<String>> progressRows = new ArrayList<>();
    SortedSet<Integer> keys = new TreeSet<Integer>(progressMap.keySet());
    for (Integer stage : keys) {
      StageData progress = progressMap.get(stage);
      final int complete = progress.numCompleteTasks();
      final int total = progress.numTasks();
      final int running = progress.numActiveTasks();
      final int failed = progress.numFailedTasks();

      SparkSQLCLIMonitor.StageState state =
          total > 0 ? SparkSQLCLIMonitor.StageState.PENDING : SparkSQLCLIMonitor.StageState.FINISHED;
      if (complete > 0 || running > 0 || failed > 0) {
        if (complete < total) {
          state = SparkSQLCLIMonitor.StageState.RUNNING;
        } else {
          state = SparkSQLCLIMonitor.StageState.FINISHED;
        }
      }
      String attempt = String.valueOf(progress.attemptId());
      String stageName = "Stage-" + stage;
      String nameWithProgress = getNameWithProgress(stageName, complete, total);
      final int pending = total - complete - running;

      progressRows.add(Arrays
          .asList(nameWithProgress, attempt, state.toString(), String.valueOf(total), String.valueOf(complete),
              String.valueOf(running), String.valueOf(pending), String.valueOf(failed), ""));
    }
    return progressRows;
  }

  @Override
  public String footerSummary() {
    return String.format("STAGES: %02d/%02d", getCompletedStages(), progressMap.keySet().size());
  }

  @Override
  public long startTime() {
    return startTime;
  }

  @Override
  public String executionStatus() {
    if (getCompletedStages() == progressMap.keySet().size()) {
      return SparkSQLCLIMonitor.StageState.FINISHED.toString();
    } else {
      return SparkSQLCLIMonitor.StageState.RUNNING.toString();
    }
  }

  @Override
  public double progressedPercentage() {

    SortedSet<Integer> keys = new TreeSet<Integer>(progressMap.keySet());
    int sumTotal = 0;
    int sumComplete = 0;
    for (Integer stage : keys) {
      StageData progress = progressMap.get(stage);
      final int complete = progress.numCompleteTasks();
      final int total = progress.numTasks();
      sumTotal += total;
      sumComplete += complete;
    }
    double progress = (sumTotal == 0) ? 1.0f : (float) sumComplete / (float) sumTotal;
    return progress;
  }

  private int getCompletedStages() {
    int completed = 0;
    SortedSet<Integer> keys = new TreeSet<Integer>(progressMap.keySet());
    for (Integer stage : keys) {
      StageData progress = progressMap.get(stage);
      final int complete = progress.numCompleteTasks();
      final int total = progress.numTasks();
      if (total > 0 && complete == total) {
        completed++;
      }
    }
    return completed;
  }

  private String getNameWithProgress(String s, int complete, int total) {

    if (s == null) {
      return "";
    }
    float percent = total == 0 ? 1.0f : (float) complete / (float) total;
    // lets use the remaining space in column 1 as progress bar
    int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
    String trimmedVName = s;

    // if the vertex name is longer than column 1 width, trim it down
    if (s.length() > COLUMN_1_WIDTH) {
      trimmedVName = s.substring(0, COLUMN_1_WIDTH - 2);
      trimmedVName += "..";
    } else {
      trimmedVName += " ";
    }
    StringBuilder result = new StringBuilder(trimmedVName);
    int toFill = (int) (spaceRemaining * percent);
    for (int i = 0; i < toFill; i++) {
      result.append(".");
    }
    return result.toString();
  }
}
