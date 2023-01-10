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

package alluxio.multi.process;

import alluxio.conf.PropertyKey;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for running and interacting with an Alluxio master in a separate process.
 */
@ThreadSafe
public class Master extends BaseMaster {
  /**
   * @param logsDir logs directory
   * @param properties alluxio properties
   */
  public Master(File logsDir, Map<PropertyKey, Object> properties) throws IOException {
    super(logsDir, properties);
  }

  @Override
  int getPort() {
    return (int) mProperties.get(PropertyKey.MASTER_RPC_PORT);
  }

  @Override
  Class<?> getProcessClass() {
    return LimitedLifeMasterProcess.class;
  }
}
