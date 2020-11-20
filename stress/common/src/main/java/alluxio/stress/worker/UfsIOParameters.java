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

package alluxio.stress.worker;

import alluxio.stress.Parameters;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters used in the UFS I/O throughput test.
 * */
public class UfsIOParameters extends Parameters {
  public static final String USE_MOUNT_CONF = "--use-mount-conf";
  public static final String CONF = "--conf";
  public static final String PATH = "--path";

  @Parameter(names = {"--threads"}, description = "the number of threads to use")
  public int mThreads = 4;

  @Parameter(names = {"--io-size"},
          description = "size of data to write and then read for each thread")
  public String mDataSize = "4G";

  @Parameter(names = {PATH},
          description = "the Ufs Path to write temporary data in",
          required = true)
  public String mPath;

  @Parameter(names = {USE_MOUNT_CONF},
      description = "If true, attempt to load the ufs configuration from an existing mount point "
          + "to read/write to the base path, it will override the configuration specified through "
          + "--conf parameter")
  public boolean mUseUfsConf = false;

  @DynamicParameter(names = CONF,
      description = "Any HDFS client configuration key=value. Can repeat to provide multiple "
          + "configuration values.")
  public Map<String, String> mConf = new HashMap<>();
}
