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

package alluxio.underfs.swift;

import alluxio.AlluxioURI;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SwiftUnderFileSystem}.
 */
public class SwiftUnderFileSystemTest {

  private static final String SCHEME = "swift://";

  /**
   * Test case for {@link SwiftUnderFileSystem#getContainerName(AlluxioURI)}.
   */
  @Test
  public void getContainerName() {
    // Test valid container names
    String[] validContainerNames = {"container", "swift_container", "a@b:123", "a.b.c"};
    for (String containerName : validContainerNames) {
      AlluxioURI uri = new AlluxioURI(SCHEME + containerName);
      Assert.assertTrue(containerName.equals(SwiftUnderFileSystem.getContainerName(uri)));
    }

    // Test separator splits container name
    String[] paths = new String[validContainerNames.length];
    for (int i = 0; i < paths.length; ++i) {
      paths[i] = validContainerNames[i] + "/folder/file";
    }
    for (int i = 0; i < paths.length; ++i) {
      AlluxioURI uri = new AlluxioURI(SCHEME + paths[i]);
      Assert.assertTrue(validContainerNames[i].equals(SwiftUnderFileSystem.getContainerName(uri)));
    }
  }
}
