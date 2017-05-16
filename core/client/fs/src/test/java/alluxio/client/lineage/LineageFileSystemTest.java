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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.exception.status.NotFoundException;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests {@link LineageFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ LineageContext.class, LineageMasterClient.class, FileSystemContext.class,
    FileSystemMasterClient.class })
public final class LineageFileSystemTest {
  private static final long TEST_BLOCK_SIZE = Constants.MB;

  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private LineageFileSystem mAlluxioLineageFileSystem;

  /**
   * Sets up all dependencies before running a test.
   */
  @Before
  public void before() throws Exception {
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);

    FileSystemContext fileSystemContext = PowerMockito.mock(FileSystemContext.class);
    Mockito.when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);

    FileSystemMasterClient
        fileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(fileSystemContext.acquireMasterClient()).thenReturn(fileSystemMasterClient);
    Mockito.when(fileSystemMasterClient
        .getStatus(Mockito.any(AlluxioURI.class), Mockito.any(GetStatusOptions.class)))
        .thenReturn(new URIStatus(new FileInfo()));

    mAlluxioLineageFileSystem = LineageFileSystem.get(fileSystemContext, mLineageContext);
  }

  /**
   * Tests that a {@link LineageFileOutStream} is returned.
   */
  @Test
  public void getLineageOutStream() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito.when(
        mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0, TtlAction.FREE))
        .thenReturn(1L);
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE)
        .setTtl(0).setTtlAction(alluxio.wire.TtlAction.FREE);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream instanceof LineageFileOutStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that a {@link DummyFileOutputStream} is returned.
   */
  @Test
  public void getDummyOutStream() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito.when(
        mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0, TtlAction.DELETE))
        .thenReturn(-1L);
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE)
        .setTtl(0);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that a {@link FileOutStream} is returned.
   */
  @Test
  public void getNonLineageStream() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    Mockito
        .when(mLineageMasterClient.reinitializeFile("test", TEST_BLOCK_SIZE, 0,
            TtlAction.DELETE))
        .thenThrow(new NotFoundException("lineage does not exist"));
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(TEST_BLOCK_SIZE)
        .setTtl(0);
    FileOutStream outStream = mAlluxioLineageFileSystem.createFile(path, options);
    Assert.assertTrue(outStream != null);
    Assert.assertFalse(outStream instanceof LineageFileOutStream);
    Assert.assertFalse(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  /**
   * Tests that reporting a lost file from the file system informs the client about this file.
   */
  @Test
  public void reportLostFile() throws Exception {
    AlluxioURI path = new AlluxioURI("test");
    mAlluxioLineageFileSystem.reportLostFile(path);
    Mockito.verify(mLineageMasterClient).reportLostFile("test");
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
