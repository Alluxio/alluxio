/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop.contract;

import alluxio.LocalAlluxioClusterResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Rule;

public class FileSystemContractCreateIntegrationTest extends AbstractContractCreateTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource = new LocalAlluxioClusterResource();

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new FileSystemContract(conf, mClusterResource.get());
  }
}
