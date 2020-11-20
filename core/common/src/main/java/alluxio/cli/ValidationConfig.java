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

package alluxio.cli;

/**
 * Configuration settings for the validation tools.
 */
public class ValidationConfig {
  // Hms validation tool config
  public static final String HMS_TOOL_TYPE = "hms";
  public static final String METASTORE_URI_CONFIG_NAME = "metastore_uri";
  public static final String DATABASE_CONFIG_NAME = "database";
  public static final String TABLES_CONFIG_NAME = "tables";
  public static final String SOCKET_TIMEOUT_CONFIG_NAME = "socket_timeout";
  public static final String METASTORE_URI_OPTION_NAME = "m";
  public static final String DATABASE_OPTION_NAME = "d";
  public static final String TABLES_OPTION_NAME = "t";
  public static final String SOCKET_TIMEOUT_OPTION_NAME = "st";
  // Hdfs validation tool config
  public static final String HDFS_TOOL_TYPE = "hdfs";
  public static final String UFS_PATH = "ufsPath";
  public static final String UFS_CONFIG = "ufsConfig";
}
