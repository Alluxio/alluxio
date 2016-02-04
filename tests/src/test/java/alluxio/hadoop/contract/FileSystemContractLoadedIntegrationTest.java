/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.hadoop.contract;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.junit.Rule;
import org.junit.Test;

import alluxio.LocalAlluxioClusterResource;

public class FileSystemContractLoadedIntegrationTest extends AbstractFSContractTestBase {
  @Rule
  public LocalAlluxioClusterResource mClusterResource = new LocalAlluxioClusterResource();

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new FileSystemContract(conf, mClusterResource.get());
  }

  @Test
  public void testContractWorks() throws Throwable {
    String key = getContract().getConfKey(SUPPORTS_ATOMIC_RENAME);
    assertNotNull("not set: " + key, getContract().getConf().get(key));
    assertFalse("true: " + key, getContract().isSupported(SUPPORTS_ATOMIC_RENAME, false));
  }

  @Test
  public void testContractResourceOnClasspath() throws Throwable {
    URL url = this.getClass().getClassLoader().getResource(FileSystemContract.CONTRACT_XML);
    assertNotNull("could not find contract resource", url);
  }
}
