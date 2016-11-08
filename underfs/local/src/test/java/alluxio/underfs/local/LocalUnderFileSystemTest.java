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

package alluxio.underfs.local;

import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Unit tests for the {@link LocalUnderFileSystem}.
 */
public class LocalUnderFileSystemTest {
  private String mLocalUfsRoot;
  private UnderFileSystem mLocalUfs;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    mLocalUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs = LocalUnderFileSystem.get(mLocalUfsRoot);
  }

  @Test
  public void getFileLocations() throws IOException {
    byte[] bytes = getBytes();
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());

    OutputStream os = mLocalUfs.create(filepath);
    os.write(bytes);
    os.close();

    List<String> fileLocations = mLocalUfs.getFileLocations(filepath);
    Assert.assertEquals(1, fileLocations.size());
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(), fileLocations.get(0));
  }

  private byte[] getBytes() {
    String s = "BYTES";
    return s.getBytes();
  }

  private String getUniqueFileName() {
    long time = System.nanoTime();
    String fileName = "" + time;
    return fileName;
  }
}
