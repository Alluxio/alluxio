/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.lineage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystemContext;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.LineageDoesNotExistException;

/**
 * Tests {@link TachyonLineageFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageContext.class, LineageMasterClient.class, FileSystemContext.class,
    FileSystemMasterClient.class})
public final class TachyonLineageFileSystemTest {
  private static final long DEFAULT_BLOCK_SIZE = Constants.MB;

  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private TachyonLineageFileSystem mTachyonLineageFileSystem;
  private FileSystemContext mFileSystemContext;
  private FileSystemMasterClient mFileSystemMasterClient;

  @Before
  public void before() throws Exception {
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);
    Mockito.when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);
    Whitebox.setInternalState(LineageContext.class, "INSTANCE", mLineageContext);
    mTachyonLineageFileSystem = TachyonLineageFileSystem.get();
    Whitebox.setInternalState(mTachyonLineageFileSystem, "mLineageContext", mLineageContext);
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(mFileSystemContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mFileSystemContext);
    Whitebox.setInternalState(mTachyonLineageFileSystem, "mContext", mFileSystemContext);
  }

  @Test
  public void getInstanceTest() {
    TachyonLineageFileSystem lfs = TachyonLineageFileSystem.get();
    // same as the second get
    Assert.assertEquals(lfs, TachyonLineageFileSystem.get());
  }

  @Test
  public void getLineageOutStreamTest() throws Exception {
    TachyonURI path = new TachyonURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", DEFAULT_BLOCK_SIZE, 0))
        .thenReturn(1L);
    OutStreamOptions options =
        new OutStreamOptions.Builder().setBlockSizeBytes(DEFAULT_BLOCK_SIZE).setTTL(0).build();
    FileOutStream outStream = mTachyonLineageFileSystem.getOutStream(path, options);
    Assert.assertTrue(outStream instanceof LineageFileOutStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void getDummyOutStreamTest() throws Exception {
    TachyonURI path = new TachyonURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", DEFAULT_BLOCK_SIZE, 0))
        .thenReturn(-1L);
    OutStreamOptions options =
        new OutStreamOptions.Builder().setBlockSizeBytes(DEFAULT_BLOCK_SIZE).setTTL(0).build();
    FileOutStream outStream = mTachyonLineageFileSystem.getOutStream(path, options);
    Assert.assertTrue(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void getNonLineageStreamTest() throws Exception {
    TachyonURI path = new TachyonURI("test");
    Mockito.when(mLineageMasterClient.reinitializeFile("test", DEFAULT_BLOCK_SIZE, 0))
        .thenThrow(new LineageDoesNotExistException("lineage does not exist"));

    OutStreamOptions options =
        new OutStreamOptions.Builder().setBlockSizeBytes(DEFAULT_BLOCK_SIZE).setTTL(0).build();
    FileOutStream outStream = mTachyonLineageFileSystem.getOutStream(path, options);
    Assert.assertTrue(outStream instanceof FileOutStream);
    Assert.assertFalse(outStream instanceof LineageFileOutStream);
    Assert.assertFalse(outStream instanceof DummyFileOutputStream);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void reportLostFileTest() throws Exception {
    TachyonURI path = new TachyonURI("test");
    mTachyonLineageFileSystem.reportLostFile(path);
    Mockito.verify(mLineageMasterClient).reportLostFile("test");
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
