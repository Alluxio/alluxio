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

package alluxio.stress;

import alluxio.Constants;

/**
 * Constants for stress tests.
 */
public final class StressConstants {
  public static final int MAX_TIME_COUNT = 20;
  public static final int JOB_SERVICE_MAX_RESPONSE_TIME_COUNT = 1;

  /** The response time histogram can record values up to this amount. */
  public static final long TIME_HISTOGRAM_MAX = Constants.SECOND_NANO * 60 * 30;
  public static final int TIME_HISTOGRAM_PRECISION = 3;

  public static final int TIME_HISTOGRAM_COMPRESSION_LEVEL = 9;
  public static final int TIME_99_COUNT = 6;
}
