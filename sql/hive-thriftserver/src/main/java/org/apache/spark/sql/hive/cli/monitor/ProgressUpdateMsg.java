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


import java.util.List;

public class ProgressUpdateMsg {

  private List<String> headerNames;
  private List<List<String>> rows;
  private double progressedPercentage;
  private String footerSummary;
  private long startTime;

  public ProgressUpdateMsg(ProgressMonitor monitor) {
    this.headerNames = monitor.headers();
    this.rows = monitor.rows();
    this.progressedPercentage = monitor.progressedPercentage();
    this.footerSummary = monitor.footerSummary();
    this.startTime = monitor.startTime();
  }

  public ProgressUpdateMsg(
      List<String> headerNames,
      List<List<String>> rows,
      double progressedPercentage,
      CLISQLStatus status,
      String footerSummary,
      long startTime) {
    this.headerNames = headerNames;
    this.rows = rows;
    this.progressedPercentage = progressedPercentage;
    this.footerSummary = footerSummary;
    this.startTime = startTime;
  }

  public List<String> getHeaderNames() {
    return headerNames;
  }

  public List<List<String>> getRows() {
    return rows;
  }

  public double getProgressedPercentage() {
    return progressedPercentage;
  }

  public String getFooterSummary() {
    return footerSummary;
  }

  public long getStartTime() {
    return startTime;
  }
}
