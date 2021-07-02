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
 * Project constants from compilation time by maven.
 */
public final class ProjectConstants {
  /* Project version, specified in maven property. **/
  public static final String VERSION = "${project.version}";
  /* The latest git revision of at the time of building**/
  public static final String REVISION = "${git.revision}";
  /* Hadoop version, specified in maven property. **/
  public static final String HADOOP_VERSION = "${hadoop.version}";
  /* Whether update check is enabled. **/
  public static final String UPDATE_CHECK_ENABLED = "${update.check.enabled}";
  /* Update check host. **/
  public static final String UPDATE_CHECK_HOST = "${update.check.host}";
  /* Update check auth string. **/
  public static final String UPDATE_CHECK_MAGIC_NUMBER = "${update.check.auth.string}";

  private ProjectConstants() {} // prevent instantiation
}
