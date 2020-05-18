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

import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Examples for S3A specific under filesystem operations. This class tests
 * S3A specific operations like streaming upload.
 */
public final class S3ASpecificOperations {
  // A safe test wait time to wait for all parts upload
  private static final long TEST_WAIT_TIME = 10000;
  private static final byte[] TEST_BYTES = "TestBytesInS3AFiles".getBytes();

  private final InstancedConfiguration mConfiguration;
  // A child directory in the S3A bucket to run tests against
  private final String mTestDirectory;
  private final UnderFileSystem mUfs;

  /**
   * @param testDirectory the directory to run tests against
   * @param ufs the S3A under file system
   * @param configuration the instance configuration
   */
  public S3ASpecificOperations(String testDirectory,
      UnderFileSystem ufs, InstancedConfiguration configuration) {
    mTestDirectory = testDirectory;
    mUfs = ufs;
    mConfiguration = configuration;
  }

  /**
   * Test for creating an empty file using streaming upload.
   */
  public void createEmptyFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTestDirectory, "createEmpty");
    int bytesRead;
    OutputStream o = mUfs.create(testFile);
    o.close();
    byte[] buf = new byte[0];
    bytesRead = mUfs.open(testFile).read(buf);
    if (bytesRead != 0) {
      throw new IOException("The written file should be empty but is not");
    }
  }

  /**
   * Test for creating a file with data less than one part.
   */
  @RelatedS3Operations(operations
      = {"initiateMultipartUpload", "uploadPart", "completeMultipartUpload"})
  public void createFileLessThanOnePartTest() throws IOException {
    String testFile = PathUtils.concatPath(mTestDirectory, "createOnePart");
    byte[] buf;
    int bytesRead;
    OutputStream o = mUfs.create(testFile);
    o.write(TEST_BYTES);
    o.close();
    buf = new byte[TEST_BYTES.length];
    bytesRead = mUfs.open(testFile).read(buf);
    if (TEST_BYTES.length != bytesRead || !Arrays.equals(buf, TEST_BYTES)) {
      throw new IOException("Content of the written file is incorrect");
    }
  }

  /**
   * Test for creating a multipart file.
   */
  @RelatedS3Operations(operations
      = {"initiateMultipartUpload", "uploadPart", "completeMultipartUpload", "listObjects"})
  public void createMultipartFileTest() throws IOException {
    String testParent = PathUtils.concatPath(mTestDirectory, "createParent");
    String testFile = PathUtils.concatPath(testParent, "createMultipart");
    int numCopies;
    try (OutputStream outputStream = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setCreateParent(true))) {
      numCopies = 6 * Constants.MB / TEST_BYTES.length;
      for (int i = 0; i < numCopies; ++i) {
        outputStream.write(TEST_BYTES);
      }
    }
    // Validate data written successfully and content is correct
    try (InputStream inputStream = mUfs.openExistingFile(testFile)) {
      byte[] buf = new byte[numCopies * TEST_BYTES.length];
      int offset = 0;
      while (offset < buf.length) {
        int bytesRead = inputStream.read(buf, offset, buf.length - offset);
        for (int i = 0; i < bytesRead; ++i) {
          if (TEST_BYTES[(offset + i) % TEST_BYTES.length] != buf[offset + i]) {
            throw new IOException("Content of the written file is incorrect");
          }
        }
        offset += bytesRead;
      }
    }

    // These two calls will trigger S3A list objects v1
    if (!mUfs.isDirectory(testParent)) {
      throw new IOException("The written directory does not exist or is file");
    }
    UfsStatus[] statuses = mUfs.listStatus(PathUtils.concatPath(mTestDirectory, "createParent"));
    if (statuses.length != 1) {
      throw new IOException("List status result is incorrect");
    }
  }

  /**
   * Test for creating and aborting a multipart file.
   */
  @RelatedS3Operations(operations = {"initiateMultipartUpload", "uploadPart",
      "abortMultipartUploads"})
  public void createAndAbortMultipartFileTest() throws IOException {
    // Create a multipart file but do not close it
    String testFile = PathUtils.concatPath(mTestDirectory, "createAndAbort");
    OutputStream outputStream = mUfs.create(testFile);
    int numCopies = 6 * Constants.MB / TEST_BYTES.length;
    for (int i = 0; i < numCopies; ++i) {
      outputStream.write(TEST_BYTES);
    }

    System.out.println("Waiting for the in progress upload to be finished so that we can abort it. "
        + "This file may need longer time to upload which may cause test to fail.");
    CommonUtils.sleepMs(TEST_WAIT_TIME);

    mUfs.cleanup();

    boolean getS3ExpectedError = false;
    try {
      outputStream.close();
    } catch (IOException e) {
      if (e.getMessage().contains("Part upload failed")) {
        // The in progress multipart upload has been aborted
        getS3ExpectedError = true;
      } else {
        throw e;
      }
    }
    if (!getS3ExpectedError) {
      throw new IOException("The in progress multipart upload did not be aborted.");
    }
  }
}
