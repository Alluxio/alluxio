/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.block;

import java.util.Map;

/**
 * The information of worker with labels.
 */
public final class WorkerWithLabels {
  private String mHost;
  private Map<String, String> mLabels;

  /**
   * @param host the worker host
   */
  public void setHost(String host) {
    mHost = host;
  }

  /**
   * @param labels the worker labels
   */
  public void setLabels(Map<String, String> labels) {
    mLabels = labels;
  }

  /**
   * @return the worker labels
   */
  public Map<String, String> getLabels() {
    return mLabels;
  }

  /**
   * @return the worker host
   */
  public String getHost() {
    return mHost;
  }
}
