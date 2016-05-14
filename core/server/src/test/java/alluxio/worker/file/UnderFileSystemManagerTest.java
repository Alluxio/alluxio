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

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.io.OutputStream;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.class})
public final class UnderFileSystemManagerTest {
  /** The testing session id. */
  private final long mSessionId = 1L;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /** The mock input stream returned whenever a ufs file is read. */
  private InputStream mMockInputStream;
  /** The mock output stream returned whenever a ufs file is created. */
  private OutputStream mMockOutputStream;
  /** The mock under file system client. */
  private UnderFileSystem mMockUfs;

  @Before
  public void before() throws Exception {
    mMockUfs = Mockito.mock(UnderFileSystem.class);
    mMockOutputStream = Mockito.mock(OutputStream.class);
    mMockInputStream = Mockito.mock(InputStream.class);
    Mockito.when(mMockUfs.create(Mockito.anyString())).thenReturn(mMockOutputStream);
    Mockito.when(mMockUfs.open(Mockito.anyString())).thenReturn(mMockInputStream);
    PowerMockito.mockStatic(UnderFileSystem.class);
    BDDMockito.given(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .willReturn(mMockUfs);
  }

  /**
   * Tests creating a file with the manager will call {@link UnderFileSystem#create}.
   */
  @Test
  public void createUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
  }

  /**
   * Tests creating an already existing file with the manager will throw the appropriate exception.
   */
  @Test
  public void createExistingUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileAlreadyExistsException.class);
    manager.createFile(mSessionId, new AlluxioURI(uniqPath));
  }

  /**
   * Tests completing a file with the manager will call {@link UnderFileSystem#rename}.
   */
  @Test
  public void completeUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.completeFile(mSessionId, id);
    Mockito.verify(mMockUfs).rename(Mockito.contains(uniqPath), Mockito.eq(uniqPath));
  }

  /**
   * Tests completing a non existent file throws the correct exception.
   */
  @Test
  public void completeNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.completeFile(mSessionId, -1L);
  }

  /**
   * Tests completing a file with an invalid session will fail.
   */
  @Test
  public void completeUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    manager.completeFile(-1L, id);
  }

  /**
   * Tests canceling a file with the manager will call {@link UnderFileSystem#delete}.
   */
  @Test
  public void cancelUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.cancelFile(mSessionId, id);
    Mockito.verify(mMockUfs).delete(Mockito.contains(uniqPath), Mockito.eq(false));
  }

  /**
   * Tests canceling a non existent file throws the correct exception.
   */
  @Test
  public void cancelNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.cancelFile(mSessionId, -1L);
  }

  /**
   * Tests canceling a file with an invalid session fails.
   */
  @Test
  public void cancelUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    manager.cancelFile(-1L, id);
  }

  /**
   * Tests opening a file will call {@link UnderFileSystem#exists} and succeeds when the file
   * exists.
   */
  @Test
  public void openUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    manager.openFile(mSessionId, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).exists(uniqPath);
  }

  /**
   * Tests opening a file fails when the file does not exist.
   */
  @Test
  public void openNonExistentUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(false);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.openFile(mSessionId, new AlluxioURI(uniqPath));
  }

  /**
   * Tests closing an opened file invalidates the id.
   */
  @Test
  public void closeUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(mSessionId, new AlluxioURI(uniqPath));
    manager.closeFile(mSessionId, id);
    mThrown.expect(FileDoesNotExistException.class);
    manager.closeFile(mSessionId, id);
  }

  /**
   * Tests closing an unopened file fails.
   */
  @Test
  public void closeNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.closeFile(mSessionId, -1L);
  }

  /**
   * Tests closing an opened file with an invalid session fails.
   */
  @Test
  public void closeUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(mSessionId, new AlluxioURI(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    manager.closeFile(-1L, id);
  }

  /**
   * Tests getting an output stream for a valid file returns the correct output stream.
   */
  @Test
  public void getOutputStreamTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(mSessionId, new AlluxioURI(uniqPath));
    Assert.assertEquals(mMockOutputStream, manager.getOutputStream(id));
  }

  /**
   * Tests getting an output stream from an invalid file fails.
   */
  @Test
  public void getNonExistentOutputStreamTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.getOutputStream(-1L);
  }

  /**
   * Tests getting an input stream to a valid file at the start returns the correct input stream.
   */
  @Test
  public void getInputStreamTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = 0L;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    Mockito.when(mMockInputStream.skip(position)).thenReturn(position);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(mSessionId, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, in);
    Mockito.verify(mMockInputStream).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input stream.
   */
  @Test
  public void getInputStreamAtPositionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = 100L;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    Mockito.when(mMockInputStream.skip(position)).thenReturn(position);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(mSessionId, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, in);
    Mockito.verify(mMockInputStream).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to an invalid file fails.
   */
  @Test
  public void getNonExistentInputStreamTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.getInputStreamAtPosition(-1L, 0L);
  }

  /**
   * Tests cleaning sessions clears the correct state.
   */
  @Test
  public void cleanSessionsTest() throws Exception {
    String uniqPath1 = PathUtils.uniqPath();
    String uniqPath2 = PathUtils.uniqPath();
    long secondSession = mSessionId + 1;
    long position = 0L;
    UnderFileSystemManager manager = new UnderFileSystemManager();
    Mockito.when(mMockUfs.exists(Mockito.anyString())).thenReturn(true);
    long id1 = manager.openFile(mSessionId, new AlluxioURI(uniqPath1));
    long id2 = manager.openFile(secondSession, new AlluxioURI(uniqPath2));
    // Both files should be accessible
    InputStream in1 = manager.getInputStreamAtPosition(id1, position);
    Assert.assertEquals(mMockInputStream, in1);
    InputStream in2 = manager.getInputStreamAtPosition(id2, position);
    Assert.assertEquals(mMockInputStream, in2);
    in1.close();
    in2.close();
    // Clean up second session
    manager.cleanupSession(secondSession);
    // First file should still be available
    in1 = manager.getInputStreamAtPosition(id1, position);
    Assert.assertEquals(mMockInputStream, in1);
    in1.close();
    // Second file should no longer be available
    mThrown.expect(FileDoesNotExistException.class);
    in2 = manager.getInputStreamAtPosition(id2, position);
  }
}
