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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;

import alluxio.uri.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Tests loading UFS metadata many times concurrently.
 */
public final class ConcurrentFileSystemMasterLoadMetadataTest {
  private FileSystem mFileSystem;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Test
  public void loadMetadataManyDirectories() throws Exception {
    String ufsPath = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    for (int i = 0; i < 5000; i++) {
      Files.createDirectory(Paths.get(ufsPath, "a" + i));
    }

    // Run 20 concurrent listStatus calls on the root.
    List<AlluxioURI> paths = Collections.nCopies(20, new AlluxioURI("/"));

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.LIST_STATUS,
            paths.toArray(new AlluxioURI[]{}), 60 * Constants.SECOND_MS);
    assertEquals(Collections.EMPTY_LIST, errors);
  }

  @Test
  public void loadMetadataManyFiles() throws Exception {
    String ufsPath = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    for (int i = 0; i < 5000; i++) {
      Files.createFile(Paths.get(ufsPath, "a" + i));
    }

    // Run 20 concurrent listStatus calls on the root.
    List<AlluxioURI> paths = Collections.nCopies(20, new AlluxioURI("/"));

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.LIST_STATUS,
            paths.toArray(new AlluxioURI[]{}), 60 * Constants.SECOND_MS);
    assertEquals(Collections.EMPTY_LIST, errors);
  }
}
