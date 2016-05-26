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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.LineageDoesNotExistException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests {@link LineageFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageContext.class, LineageMasterClient.class, FileSystemContext.class,
    FileSystemMasterClient.class})
public final class LineageFileSystemTest {
  private static final long TEST_BLOCK_SIZE = Constants.MB;

  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private LineageFileSystem mAlluxioLineageFileSystem;

  /**
   * Sets up all dependencies before running a test.
   */
  @Before
  public void before() {
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);
    Mockito.when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);
    Whitebox.setInternalState(LineageContext.class, "INSTANCE", mLineageContext);
    mAlluxioLineageFileSystem = LineageFileSystem.get();
    Whitebox.setInternalState(mAlluxioLineageFileSystem, "mLineageContext", mLineageContext);
    FileSystemContext fileSystemContext = PowerMockito.mock(FileSystemContext.class);
    FileSystemMasterClient fileSystemMasterClient =
        PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(fileSystemContext.acquireMasterClient()).thenReturn(fileSystemMasterClient);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", fileSystemContext);
    Whitebox.setInternalState(mAlluxioLineageFileSystem, "mContext", fileSystemContext);
  }

  /**
   * Tests that a {@link LineageFileOutStream} is returned.
   *
   * @throws Exception if reinitializing the file from the client or getting the stream fails
   */
  @Test
  public void getLineageOutStreamTest() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0))
        .thenReturn(1L);
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE).setTtl(0);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream instanceof LineageFileOutStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that a {@link DummyFileOutputStream} is returned.
   *
   * @throws Exception if reinitializing the file from the client or getting the stream fails
   */
  @Test
  public void getDummyOutStreamTest() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0))
        .thenReturn(-1L);
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE).setTtl(0);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that a {@link FileOutStream} is returned.
   *
   * @throws Exception if reinitializing the file from the client or getting the stream fails
   */
  @Test
  public void getNonLineageStreamTest() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0))
        .thenThrow(new LineageDoesNotExistException("lineage does not exist"));

    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE).setTtl(0);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream instanceof FileOutStream);
    Assert.assertFalse(outStream instanceof LineageFileOutStream);
    Assert.assertFalse(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that reporting a lost file from the file system informs the client about this file.
   *
   * @throws Exception if reporting a lost file fails
   */
  @Test
  public void reportLostFileTest() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    mAlluxioLineageFileSystem.reportLostFile(path);
    Mockito.verify(mLineageMasterClient).reportLostFile("test");
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
