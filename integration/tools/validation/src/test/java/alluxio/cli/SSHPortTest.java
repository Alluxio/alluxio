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

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

public class SSHPortTest {

  @Test
  public void getHostSshPortTest() {
    InstancedConfiguration mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.HOST_SSH_PORT, 22022);

    int sshPort1 = new SshValidationTask(mConf).getHostSSHPort();
    int sshPort2 = new ClusterConfConsistencyValidationTask(mConf).getHostSSHPort();
    int sshPort3 = new ValidateEnv("", mConf).getHostSSHPort();

    assertEquals(sshPort1, 22022);
    assertEquals(sshPort2, 22022);
    assertEquals(sshPort3, 22022);
  }
}
