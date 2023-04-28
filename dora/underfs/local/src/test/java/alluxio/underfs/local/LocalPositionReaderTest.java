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

import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.PositionReaderTest;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Unit tests for the {@link LocalPositionReader}.
 */
@RunWith(Parameterized.class)
public class LocalPositionReaderTest {

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {0},
        {1},
        {128},
        {256},
        {666},
        {5314},
        { 1 * Constants.KB - 1},
        { 1 * Constants.KB},
        { 1 * Constants.KB + 1},
        { 64 * Constants.KB - 1},
        { 64 * Constants.KB},
        { 64 * Constants.KB + 1},
    });
  }

  @Parameterized.Parameter
  public int mFileLen;

  private UnderFileSystem mLocalUfs;
  private static final AlluxioConfiguration CONF = Configuration.global();

  private String mTestFile;
  private PositionReader mPositionReader;
  private PositionReaderTest mPositionReaderTest;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    String localUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs =
        UnderFileSystem.Factory.create(localUfsRoot, UnderFileSystemConfiguration.defaults(CONF));
    Path path = Paths.get(localUfsRoot, "testFile" + UUID.randomUUID());
    try (FileOutputStream os = new FileOutputStream(path.toFile())) {
      os.write(BufferUtils.getIncreasingByteArray(mFileLen));
    }
    mTestFile = path.toString();
    mPositionReader = new LocalPositionReader(mTestFile, mFileLen);
    mPositionReaderTest = new PositionReaderTest(mPositionReader, mFileLen);
  }

  @After
  public void after() throws IOException {
    mPositionReader.close();
    new File(mTestFile).delete();
    mLocalUfs.close();
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
