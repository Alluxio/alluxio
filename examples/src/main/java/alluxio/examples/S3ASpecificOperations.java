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

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.s3a.S3ALowLevelOutputStream;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Examples for S3A specific under filesystem operations. This class tests
 * S3A specific operations like streaming upload.
 */
public final class S3ASpecificOperations {
  private static final Logger LOG = LoggerFactory.getLogger(S3ASpecificOperations.class);
  private static final byte[] TEST_BYTES = "TestBytesInS3AFiles".getBytes();

  private final String mBucket;
  private final InstancedConfiguration mConfiguration;
  // A child directory in the S3A bucket to run tests against
  private final String mTestDirectory;
  private final S3AUnderFileSystem mUfs;

  /**
   * @param bucket the S3A bucket
   * @param testDirectory the directory to run tests against
   * @param ufs the S3A under file system
   * @param configuration the instance configuration
   */
  public S3ASpecificOperations(String bucket, String testDirectory,
      S3AUnderFileSystem ufs, InstancedConfiguration configuration) {
    mBucket = bucket;
    mConfiguration = configuration;
    mTestDirectory = testDirectory;
    mUfs = ufs;
  }

  /**
   * Test for creating empty file using streaming upload.
   */
  public void createEmptyFileUsingStreamingUploadTest() throws IOException {
    String testFile = PathUtils.concatPath(mTestDirectory, "createOpenEmpty");
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
   * Test for creating file less than one part.
   */
  public void createFileLessThanOnePartTest() throws IOException {
    String testFile = PathUtils.concatPath(mTestDirectory, "createOpen");
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
   * Test for creating multipart file.
   */
  public void createMultipartFileTest() throws IOException {
    String testParent = PathUtils.concatPath(mTestDirectory, "createParent");
    String testFile = PathUtils.concatPath(testParent, "createOpenLarge");
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
   * Test for creating and aborting multipart file.
   */
  public void createAndAbortMultipartFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTestDirectory, "createAndAbort");
    OutputStream outputStream = mUfs.create(testFile);
    // Clean all intermediate multipart uploads
    mUfs.cleanup();
    if (getMultipartUploadList().size() != 0) {
      throw new IOException("Failed to clean previous intermediate multipart uploads");
    }

    // Create a file but do not close it
    int numCopies = 6 * Constants.MB / TEST_BYTES.length;
    for (int i = 0; i < numCopies; ++i) {
      outputStream.write(TEST_BYTES);
    }
    // Multipart upload that has in progress upload will not be aborted
    ((S3ALowLevelOutputStream) outputStream).waitForAllPartsUpload();
    int size = getMultipartUploadList().size();
    if (size != 1) {
      throw new IOException(String.format("Expected to have one intermediate multipart upload, "
          + "but get %s in %s", size, mBucket));
    }
    mUfs.cleanup();
    if (getMultipartUploadList().size() != 0) {
      throw new IOException("Failed to clean the in progress multipart upload");
    }
  }

  /**
   * @return a list of intermediate multipart uploads
   */
  private List<MultipartUpload> getMultipartUploadList() {
    return mUfs.getS3Client().listMultipartUploads(
        new ListMultipartUploadsRequest(UnderFileSystemUtils
            .getBucketName(new AlluxioURI(mBucket)))).getMultipartUploads();
  }
}
