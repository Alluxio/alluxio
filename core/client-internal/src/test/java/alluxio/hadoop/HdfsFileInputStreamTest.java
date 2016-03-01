/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.ExceptionMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Unit tests for {@link HdfsFileInputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HdfsFileInputStream.class, AlluxioURI.class,
    Path.class, Configuration.class, Statistics.class, FileSystem.class})
public class HdfsFileInputStreamTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private static final int BUFSIZE = 1024;

  private HdfsFileInputStream mHdfsFileInputStream;
  private AlluxioURI mAlluxioURI;
  private Path mHdfsPath;
  private org.apache.hadoop.conf.Configuration mHdfsConf;
  private org.apache.hadoop.fs.FileSystem.Statistics mStats;
  private URIStatus mFileInfo;
  private FileInStream mAlluxioFileInputStream = null;

  /**
   * Sets up the context before a test runs.
   *
   * @throws Exception when the initialize fail
   */
  @Before
  public void before() throws Exception {
    mAlluxioURI = PowerMockito.mock(AlluxioURI.class);
    mHdfsPath = PowerMockito.mock(Path.class);
    mHdfsConf = PowerMockito.mock(org.apache.hadoop.conf.Configuration.class);
    mStats = PowerMockito.mock(org.apache.hadoop.fs.FileSystem.Statistics.class);
    mFileInfo = PowerMockito.mock(URIStatus.class);
    mAlluxioFileInputStream = PowerMockito.mock(FileInStream.class);

    PowerMockito.mockStatic(FileSystem.Factory.class);
    FileSystem fs = PowerMockito.mock(FileSystem.class);
    when(FileSystem.Factory.get()).thenReturn(fs);

    when(fs.getStatus(mAlluxioURI)).thenReturn(mFileInfo);
    when(fs.openFile(any(AlluxioURI.class), any(OpenFileOptions.class)))
      .thenReturn(mAlluxioFileInputStream);

    mHdfsFileInputStream = new HdfsFileInputStream(
        mAlluxioURI, mHdfsPath, mHdfsConf, BUFSIZE, mStats);
  }

  /**
   * Resets the test context.
   *
   * @throws IOException when the initialize fail
   */
  @After
  public void after() throws IOException {
    mHdfsFileInputStream.close();
  }

  /**
   * Test the {@link available()} method.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void availableTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mHdfsFileInputStream.available();
    verify(mHdfsFileInputStream).available();
  }
}
