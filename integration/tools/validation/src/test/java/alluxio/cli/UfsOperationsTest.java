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

import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests for {@link UnderFileSystemCommonOperations}.
 */
public class UfsOperationsTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void ufsContractTest() throws Exception {
    File ufsPath = mFolder.newFolder("ufsContractTest");

    try {
      UnderFileSystemContractTest
          .main(new String[] {"--path", "file://" + ufsPath.getAbsolutePath()});
    } catch (Throwable e) {
      fail("UFS contract failed: " + e.getMessage());
    }
  }
}
