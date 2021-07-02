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

package alluxio.client.fs.concurrent;

import static org.junit.Assert.assertThat;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests the correctness of concurrent operations.
 */
public final class ConcurrentFileSystemIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private FileSystem mFileSystem;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void listAndDeleteLargeDirectory() throws Exception {
    int numSubdirs = 100;
    AlluxioURI testDir = new AlluxioURI("/testDir");
    mFileSystem.createDirectory(testDir);
    for (int i = 0; i < numSubdirs; i++) {
      mFileSystem.createDirectory(new AlluxioURI("/testDir/" + i));
    }
    Thread deleteThread = new Thread(() -> {
      for (int i = 0; i < numSubdirs; i++) {
        try {
          mFileSystem.delete(new AlluxioURI("/testDir/" + i));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    deleteThread.start();
    int prevCount = numSubdirs;
    while (deleteThread.isAlive()) {
      // This should not throw an exception, even with concurrent deletes happening.
      int count = mFileSystem.listStatus(testDir).size();
      // Number of files should only decrease.
      assertThat(count, Matchers.lessThanOrEqualTo(prevCount));
      prevCount = count;
    }
    deleteThread.join();
  }
}
