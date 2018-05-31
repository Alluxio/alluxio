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

package alluxio;

/**
 * Compile time call home constants.
 */
public final class CallHomeConstants {
  /* Host to report call home information to. **/
  public static final String CALL_HOME_HOST = "${call.home.host}";
  /* Enable call home or not. **/
  public static final String CALL_HOME_ENABLED = "${call.home.enabled}";
  /* Period between two consequent call home executions. **/
  public static final String CALL_HOME_PERIOD_MS  = "${call.home.period}";

  private CallHomeConstants() {} // prevent instantiation
}
