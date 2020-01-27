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

import java.io.PrintStream;
import java.util.List;

public class CLIInPlaceUpdateStream implements InPlaceUpdateStream {
  private InPlaceUpdate inPlaceUpdate;
  private EventNotifier notifier;

  public CLIInPlaceUpdateStream(PrintStream out, InPlaceUpdateStream.EventNotifier notifier) {
    this.inPlaceUpdate = new InPlaceUpdate(out);
    this.notifier = notifier;
  }

  @Override
  public void update(ProgressUpdateMsg progressMsg) {
      inPlaceUpdate.render(new ProgressMonitorWrapper(progressMsg));
  }

  @Override
  public EventNotifier getEventNotifier() {
    return notifier;
  }

  static class ProgressMonitorWrapper implements ProgressMonitor {
    private ProgressUpdateMsg response;

    ProgressMonitorWrapper(ProgressUpdateMsg response) {
      this.response = response;
    }

    @Override
    public List<String> headers() {
      return response.getHeaderNames();
    }

    @Override
    public List<List<String>> rows() {
      return response.getRows();
    }

    @Override
    public String footerSummary() {
      return response.getFooterSummary();
    }

    @Override
    public long startTime() {
      return response.getStartTime();
    }

    @Override
    public String executionStatus() {
      throw new UnsupportedOperationException(
          "This should never be used for anything. All the required data is available via other methods"
      );
    }

    @Override
    public double progressedPercentage() {
      return response.getProgressedPercentage();
    }
  }
}
