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

package alluxio.hub.test;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.manager.process.ManagerProcessContext;
import alluxio.security.authentication.AuthType;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class BaseHubTest {
  @ClassRule
  public static final TemporaryFolder TEST_CONF_DIR = new TemporaryFolder();
  @ClassRule
  public static final TemporaryFolder TEST_PRESTO_CONF_DIR = new TemporaryFolder();

  protected static InstancedConfiguration getTestConfig() throws Exception {
    InstancedConfiguration c =
        new InstancedConfiguration(ServerConfiguration.global().copyProperties());
    c.set(PropertyKey.HUB_MANAGER_RPC_PORT, 0);
    c.set(PropertyKey.HUB_MANAGER_EXECUTOR_THREADS_MIN, 0);
    c.set(PropertyKey.HUB_AGENT_RPC_PORT, 0);
    c.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    c.set(PropertyKey.CONF_DIR, TEST_CONF_DIR.getRoot().getCanonicalPath());
    c.set(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH, TEST_PRESTO_CONF_DIR.getRoot()
            .getCanonicalPath());
    return c;
  }

  public static ManagerProcessContext getTestManagerContext() throws Exception {
    return getTestManagerContext(getTestConfig());
  }

  public static ManagerProcessContext getTestManagerContext(AlluxioConfiguration conf) {
    return new ManagerProcessContext(conf);
  }
}
