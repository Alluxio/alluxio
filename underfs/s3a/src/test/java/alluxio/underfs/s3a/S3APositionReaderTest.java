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

package alluxio.underfs.s3a;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.PositionReaderTest;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Unit tests for the {@link S3APositionReader}.
 */
@RunWith(Parameterized.class)
public class S3APositionReaderTest {
  private static final String TEST_S3A_PATH_CONF = "alluxio.test.s3a.path";

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {0},
        {1},
        {128},
        {666},
        {5314},
        { 1 * Constants.KB - 1},
        { 1 * Constants.KB},
        { 1 * Constants.KB + 1},
    });
  }

  @Parameterized.Parameter
  public int mFileLen;

  private UnderFileSystem mS3Ufs = null;
  private static final AlluxioConfiguration CONF = Configuration.global();

  private String mTestFile;
  private PositionReader mPositionReader;
  private PositionReaderTest mPositionReaderTest;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    String s3Path = System.getProperty(TEST_S3A_PATH_CONF);
    Assume.assumeTrue(s3Path != null && !s3Path.isEmpty());
    AlluxioURI ufsRoot = new AlluxioURI(s3Path).join(UUID.randomUUID().toString());
    mS3Ufs = UnderFileSystem.Factory.create(ufsRoot.toString(),
        UnderFileSystemConfiguration.defaults(CONF));
    mTestFile = ufsRoot.join(UUID.randomUUID().toString()).toString();
    try (OutputStream os = mS3Ufs.create(mTestFile)) {
      os.write(BufferUtils.getIncreasingByteArray(mFileLen));
    }
    mPositionReader = mS3Ufs.openPositionRead(mTestFile, mFileLen);
    mPositionReaderTest = new PositionReaderTest(mPositionReader, mFileLen);
  }

  @After
  public void after() throws IOException {
    if (mS3Ufs != null) {
      mPositionReader.close();
      mS3Ufs.deleteFile(mTestFile);
      mS3Ufs.close();
    }
  }

  @Test
  public void testAllCornerCases() throws IOException {
    mPositionReaderTest.testAllCornerCases();
  }

  @Test
  public void testReadRandomPart() throws IOException {
    mPositionReaderTest.testReadRandomPart();
  }

  @Test
  public void testConcurrentReadRandomPart() throws Exception {
    mPositionReaderTest.concurrentReadPart();
  }
}
