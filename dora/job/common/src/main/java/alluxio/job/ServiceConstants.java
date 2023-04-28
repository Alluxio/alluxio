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

package alluxio.job;

/**
 * Alluxio job service constants.
 */
public final class ServiceConstants {
  public static final String MASTER_SERVICE_PREFIX = "master/job";

  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";

  public static final String CANCEL = "cancel";
  public static final String FAILURE_HISTORY = "failure_history";
  public static final String GET_STATUS = "get_status";
  public static final String LIST = "list";
  public static final String RUN = "run";

  private ServiceConstants() {} // prevent instantiation
}
