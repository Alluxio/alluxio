/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job;

import java.io.Serializable;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job configuration.
 */
@ThreadSafe
public class JobConf implements Serializable {
  private static final long serialVersionUID = 1258775437399802121L;
  private final String mOutputFilePath;

  /**
   * Constructs the job configuration.
   *
   * @param outputFilePath the path of the output file
   */
  public JobConf(String outputFilePath) {
    mOutputFilePath = outputFilePath;
  }

  /**
   * @return the path file that redirects the job's stdout into
   */
  public String getOutputFilePath() {
    return mOutputFilePath;
  }
}
