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

package alluxio.job.move;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.job.JobIntegrationTest;
import alluxio.job.wire.JobInfo;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Integration test for the move job.
 */
public final class MoveIntegrationTest extends JobIntegrationTest {
  private static final byte[] TEST_BYTES = "hello".getBytes();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void moveFile() throws Exception {
    File ufsMountPoint1 = mFolder.newFolder();
    File ufsMountPoint2 = mFolder.newFolder();
    mFileSystem.mount(new AlluxioURI("/mount1"), new AlluxioURI(ufsMountPoint1.getAbsolutePath()));
    mFileSystem.mount(new AlluxioURI("/mount2"), new AlluxioURI(ufsMountPoint2.getAbsolutePath()));
    String source = "/mount1/source";
    String destination = "/mount2/destination";
    createFileWithTestBytes(source);
    long jobId = mJobMaster
        .run(new MoveConfig(source, destination, WriteType.CACHE_THROUGH.toString(), true));
    JobInfo info = waitForJobToFinish(jobId);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(source)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(destination)));
    checkFileContainsTestBytes(destination);
    // One worker task is needed when moving within the same mount point.
    Assert.assertEquals(1, info.getTaskInfoList().size());
  }

  @Test
  public void moveDirectory() throws Exception {
    File ufsMountPoint1 = mFolder.newFolder();
    File ufsMountPoint2 = mFolder.newFolder();
    mFileSystem.mount(new AlluxioURI("/mount1"), new AlluxioURI(ufsMountPoint1.getAbsolutePath()));
    mFileSystem.mount(new AlluxioURI("/mount2"), new AlluxioURI(ufsMountPoint2.getAbsolutePath()));
    mFileSystem.createDirectory(new AlluxioURI("/mount1/source"));
    createFileWithTestBytes("/mount1/source/foo");
    createFileWithTestBytes("/mount1/source/bar");
    mFileSystem.createDirectory(new AlluxioURI("/mount1/source/baz"));
    createFileWithTestBytes("/mount1/source/baz/bat");
    long jobId = mJobMaster.run(new MoveConfig("/mount1/source", "/mount2/destination",
        WriteType.CACHE_THROUGH.toString(), true));
    waitForJobToFinish(jobId);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/mount1/source")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/mount2/destination")));
    checkFileContainsTestBytes("/mount2/destination/foo");
    checkFileContainsTestBytes("/mount2/destination/bar");
    checkFileContainsTestBytes("/mount2/destination/baz/bat");
  }

  /**
   * Creates a file with the given name containing TEST_BYTES.
   */
  private void createFileWithTestBytes(String filename) throws Exception {
    try (FileOutStream out = mFileSystem.createFile(new AlluxioURI(filename))) {
      out.write(TEST_BYTES);
    }
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(filename)));
  }

  /**
   * Checks that the given file contains TEST_BYTES.
   */
  private void checkFileContainsTestBytes(String filename) throws Exception {
    try (FileInStream in = mFileSystem.openFile(new AlluxioURI(filename))) {
      Assert.assertArrayEquals(TEST_BYTES, IOUtils.toByteArray(in));
    }
  }
}
