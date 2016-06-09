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

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.PreconditionMessage;
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
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.io.OutputStream;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.class})
public final class UnderFileSystemManagerTest {
  /** An invalid session id. */
  private static final long INVALID_SESSION_ID = -1L;
  /** An invalid file id. */
  private static final long INVALID_FILE_ID = -2L;
  /** The testing session id. */
  private static final long SESSION_ID = 1L;
  /** Mock length of the file. */
  private static final long FILE_LENGTH = 1000L;

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
    Mockito.when(mMockUfs.rename(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    Mockito.when(mMockUfs.getFileSize(Mockito.anyString())).thenReturn(FILE_LENGTH);
    PowerMockito.mockStatic(UnderFileSystem.class);
    BDDMockito.given(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .willReturn(mMockUfs);
  }

  /**
   * Tests creating a file with the manager will call {@link UnderFileSystem#create} and
   * {@link UnderFileSystem#connectFromWorker}.
   */
  @Test
  public void createUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    Mockito.verify(mMockUfs).connectFromWorker(Mockito.any(Configuration.class),
        Mockito.anyString());
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
    mThrown.expectMessage(ExceptionMessage.FAILED_UFS_CREATE.getMessage(uniqPath));
    manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
  }

  /**
   * Tests completing a file with the manager will call {@link UnderFileSystem#rename}.
   */
  @Test
  public void completeUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.completeFile(SESSION_ID, id, null, null);
    Mockito.verify(mMockUfs).rename(Mockito.contains(uniqPath), Mockito.eq(uniqPath));
  }

  /**
   * Tests completing a file with user and group.
   */
  @Test
  public void completeUfsFileWithOwnerTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    String user = "User";
    String group = "Group";
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.completeFile(SESSION_ID, id, user, group);
    Mockito.verify(mMockUfs).rename(Mockito.contains(uniqPath), Mockito.eq(uniqPath));
    Mockito.verify(mMockUfs).setOwner(uniqPath, user, group);
  }

  /**
   * Tests completing a non existent file throws the correct exception.
   */
  @Test
  public void completeNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    manager.completeFile(SESSION_ID, INVALID_FILE_ID, null, null);
  }

  /**
   * Tests completing a file with an invalid session will fail.
   */
  @Test
  public void completeUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "complete"));
    manager.completeFile(INVALID_SESSION_ID, id, null, null);
  }

  /**
   * Tests canceling a file with the manager will call {@link UnderFileSystem#delete}.
   */
  @Test
  public void cancelUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.cancelFile(SESSION_ID, id);
    Mockito.verify(mMockUfs).delete(Mockito.contains(uniqPath), Mockito.eq(false));
  }

  /**
   * Tests canceling a non existent file throws the correct exception.
   */
  @Test
  public void cancelNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    manager.cancelFile(SESSION_ID, INVALID_FILE_ID);
  }

  /**
   * Tests canceling a file with an invalid session fails.
   */
  @Test
  public void cancelUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "cancel"));
    manager.cancelFile(INVALID_SESSION_ID, id);
  }

  /**
   * Tests opening a file will call {@link UnderFileSystem#exists} and
   * {@link UnderFileSystem#connectFromWorker}and succeeds when the file exists.
   */
  @Test
  public void openUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).exists(uniqPath);
    Mockito.verify(mMockUfs).connectFromWorker(Mockito.any(Configuration.class),
        Mockito.anyString());
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
    mThrown.expectMessage(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(uniqPath));
    manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
  }

  /**
   * Tests closing an opened file invalidates the id.
   */
  @Test
  public void closeUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    manager.closeFile(SESSION_ID, id);
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(id));
    manager.closeFile(SESSION_ID, id);
  }

  /**
   * Tests closing an unopened file fails.
   */
  @Test
  public void closeNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    manager.closeFile(SESSION_ID, INVALID_FILE_ID);
  }

  /**
   * Tests closing an opened file with an invalid session fails.
   */
  @Test
  public void closeUfsFileInvalidSessionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String
        .format(PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "close"));
    manager.closeFile(INVALID_SESSION_ID, id);
  }

  /**
   * Tests getting an output stream for a valid file returns the correct output stream.
   */
  @Test
  public void getOutputStreamTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(SESSION_ID, new AlluxioURI(uniqPath));
    Assert.assertEquals(mMockOutputStream, manager.getOutputStream(id));
  }

  /**
   * Tests getting an output stream from an invalid file fails.
   */
  @Test
  public void getNonExistentOutputStreamTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    manager.getOutputStream(INVALID_FILE_ID);
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
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in));
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input stream.
   */
  @Test
  public void getInputStreamAtPositionTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = FILE_LENGTH - 1;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    Mockito.when(mMockInputStream.skip(position)).thenReturn(position);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in));
    Mockito.verify(mMockInputStream).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input
   * stream, then reading and getting the next position returns the same stream.
   */
  @Test
  public void getInputStreamAtPositionCacheTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = 0;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in));
    long nextPosition = 100;
    Mockito.when(mMockInputStream.skip(nextPosition)).thenReturn(nextPosition);
    in.skip(nextPosition - position);
    InputStream in2 = manager.getInputStreamAtPosition(id, nextPosition);
    Assert.assertEquals(in, in2);
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input
   * stream, then reading a backward position gets another input stream.
   */
  @Test
  public void getInputStreamAtPositionInvalidCacheTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = 0;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in));
    long nextPosition = 100;
    Mockito.when(mMockInputStream.skip(nextPosition)).thenReturn(nextPosition);
    in.skip(nextPosition - position);
    InputStream in2 = manager.getInputStreamAtPosition(id, position);
    Assert.assertNotEquals(in, in2);
    in.close();
    in2.close();
  }

  /**
   * Tests getting an input stream at EOF returns null.
   */
  @Test
  public void getInputStreamAtEOFTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    long position = FILE_LENGTH;
    Mockito.when(mMockUfs.exists(uniqPath)).thenReturn(true);
    Mockito.when(mMockInputStream.skip(position)).thenReturn(position);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath));
    InputStream in = manager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(null, in);
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
  }

  /**
   * Tests getting an input stream to an invalid file fails.
   */
  @Test
  public void getNonExistentInputStreamTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    manager.getInputStreamAtPosition(INVALID_FILE_ID, 0L);
  }

  /**
   * Tests cleaning sessions clears the correct state.
   */
  @Test
  public void cleanSessionsTest() throws Exception {
    String uniqPath1 = PathUtils.uniqPath();
    String uniqPath2 = PathUtils.uniqPath();
    long secondSessionId = SESSION_ID + 1;
    long position = 0L;
    UnderFileSystemManager manager = new UnderFileSystemManager();
    Mockito.when(mMockUfs.exists(Mockito.anyString())).thenReturn(true);
    long id1 = manager.openFile(SESSION_ID, new AlluxioURI(uniqPath1));
    long id2 = manager.openFile(secondSessionId, new AlluxioURI(uniqPath2));
    // Both files should be accessible
    InputStream in1 = manager.getInputStreamAtPosition(id1, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in1));
    InputStream in2 = manager.getInputStreamAtPosition(id2, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in2));
    in1.close();
    in2.close();
    // Clean up second session
    manager.cleanupSession(secondSessionId);
    // First file should still be available
    in1 = manager.getInputStreamAtPosition(id1, position);
    Assert.assertEquals(mMockInputStream, getInternalInputStream(in1));
    in1.close();
    // Second file should no longer be available
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(id2));
    manager.getInputStreamAtPosition(id2, position);
  }

  /**
   * Used to enable equality checks on the underlying stream.
   *
   * @param in the wrapper stream
   * @return the internal input stream of a wrapper input stream.
   */
  private InputStream getInternalInputStream(InputStream in) {
    return (InputStream) Whitebox.getInternalState(in, "in");
  }
}
