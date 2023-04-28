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

package alluxio.cli.fsadmin;

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests on {@link FileSystemAdminShellUtils}.
 */
public class FileSystemAdminShellUtilsTest {

  @Test
  public void compareTierNames() {
    Assert.assertTrue("MEM should be placed before SSD",
        FileSystemAdminShellUtils.compareTierNames(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD) < 0);
    Assert.assertTrue("MEM should be placed before HDD",
        FileSystemAdminShellUtils.compareTierNames(Constants.MEDIUM_MEM, Constants.MEDIUM_HDD) < 0);
    Assert.assertTrue("HDD should be placed after SSD",
        FileSystemAdminShellUtils.compareTierNames(Constants.MEDIUM_HDD, Constants.MEDIUM_SSD) > 0);
    Assert.assertTrue("HDD should be placed before DOM",
        FileSystemAdminShellUtils.compareTierNames("DOM", Constants.MEDIUM_HDD) > 0);
    Assert.assertTrue("RAM should be placed after DOM",
        FileSystemAdminShellUtils.compareTierNames("RAM", "DOM") > 0);
  }
}
