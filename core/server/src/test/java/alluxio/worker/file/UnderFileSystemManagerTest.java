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
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.PreconditionMessage;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

  /** The manager object to test. */
  private UnderFileSystemManager mManager;
  /** The mock input stream returned whenever a ufs file is read. */
  private InputStream mMockInputStream;
  /** The mock output stream returned whenever a ufs file is created. */
  private OutputStream mMockOutputStream;
  /** The mock under file system client. */
  private UnderFileSystem mMockUfs;
  /** The filename in the under storage. */
  private AlluxioURI mUri;

  @Before
  public void before() throws Exception {
    mManager = new UnderFileSystemManager();
    mMockUfs = Mockito.mock(UnderFileSystem.class);
    mMockOutputStream = Mockito.mock(OutputStream.class);
    mMockInputStream = Mockito.mock(InputStream.class);
    mUri = new AlluxioURI(PathUtils.uniqPath());
    Mockito.when(mMockUfs.create(Mockito.anyString())).thenReturn(mMockOutputStream);
    Mockito.when(mMockUfs.create(Mockito.anyString(),
        Mockito.any(CreateOptions.class))).thenReturn(mMockOutputStream);
    Mockito.when(mMockUfs.open(Mockito.anyString())).thenReturn(mMockInputStream);
    Mockito.when(mMockUfs.rename(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    Mockito.when(mMockUfs.getFileSize(Mockito.anyString())).thenReturn(FILE_LENGTH);
    PowerMockito.mockStatic(UnderFileSystem.class);
    Mockito.when(UnderFileSystem.get(Mockito.anyString())).thenReturn(mMockUfs);
    Mockito.when(mMockInputStream.skip(Mockito.anyInt())).thenAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        return args[0];
      }
    });
  }

  /**
   * Tests creating a file with the manager will call {@link UnderFileSystem#create} and
   * {@link UnderFileSystem#connectFromWorker}.
   */
  @Test
  public void createUfsFile() throws Exception {
    mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    Mockito.verify(mMockUfs).create(Mockito.contains(mUri.toString()),
        Mockito.any(CreateOptions.class));
    Mockito.verify(mMockUfs).connectFromWorker(Mockito.anyString());
  }

  /**
   * Tests completing a file with the manager will call {@link UnderFileSystem#rename}.
   */
  @Test
  public void completeUfsFile() throws Exception {
    long id = mManager.createFile(SESSION_ID, mUri,
        new Permission("", "", Constants.DEFAULT_FILE_SYSTEM_MODE));
    mManager.completeFile(SESSION_ID, id,
        new Permission("", "", Constants.DEFAULT_FILE_SYSTEM_MODE));
    Mockito.verify(mMockUfs).rename(Mockito.contains(mUri.toString()), Mockito.eq(mUri.toString()));
  }

  /**
   * Tests completing a file with user and group.
   */
  @Test
  public void completeUfsFileWithOwner() throws Exception {
    String user = "User";
    String group = "Group";
    long id = mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    mManager.completeFile(SESSION_ID, id,
        new Permission(user, group, Mode.createFullAccess()));
    Mockito.verify(mMockUfs).setOwner(mUri.toString(), user, group);
  }

  /**
   * Tests completing a non existent file throws the correct exception.
   */
  @Test
  public void completeNonExistentUfsFile() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    mManager.completeFile(SESSION_ID, INVALID_FILE_ID, Permission.defaults());
  }

  /**
   * Tests completing a file with an invalid session will fail.
   */
  @Test
  public void completeUfsFileInvalidSession() throws Exception {
    long id = mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "complete"));
    mManager.completeFile(INVALID_SESSION_ID, id, Permission.defaults());
  }

  /**
   * Tests canceling a file with the manager will call {@link UnderFileSystem#delete}.
   */
  @Test
  public void cancelUfsFile() throws Exception {
    long id = mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    mManager.cancelFile(SESSION_ID, id);
    Mockito.verify(mMockUfs).delete(Mockito.contains(mUri.toString()), Mockito.eq(false));
  }

  /**
   * Tests canceling a non existent file throws the correct exception.
   */
  @Test
  public void cancelNonExistentUfsFile() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    mManager.cancelFile(SESSION_ID, INVALID_FILE_ID);
  }

  /**
   * Tests canceling a file with an invalid session fails.
   */
  @Test
  public void cancelUfsFileInvalidSession() throws Exception {
    long id = mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(
        PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "cancel"));
    mManager.cancelFile(INVALID_SESSION_ID, id);
  }

  /**
   * Tests opening a file will call {@link UnderFileSystem#exists} and
   * {@link UnderFileSystem#connectFromWorker}and succeeds when the file exists.
   */
  @Test
  public void openUfsFile() throws Exception {
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    mManager.openFile(SESSION_ID, new AlluxioURI(mUri.toString()));
    Mockito.verify(mMockUfs).exists(mUri.toString());
    Mockito.verify(mMockUfs).connectFromWorker(Mockito.anyString());
  }

  /**
   * Tests opening a file fails when the file does not exist.
   */
  @Test
  public void openNonExistentUfsFile() throws Exception {
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(false);
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(mUri.toString()));
    mManager.openFile(SESSION_ID, mUri);
  }

  /**
   * Tests closing an open file invalidates the id.
   */
  @Test
  public void closeUfsFile() throws Exception {
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, new AlluxioURI(mUri.toString()));
    mManager.closeFile(SESSION_ID, id);
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(id));
    mManager.closeFile(SESSION_ID, id);
  }

  /**
   * Tests closing an unopened file fails.
   */
  @Test
  public void closeNonExistentUfsFile() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    mManager.closeFile(SESSION_ID, INVALID_FILE_ID);
  }

  /**
   * Tests closing an opened file with an invalid session fails.
   */
  @Test
  public void closeUfsFileInvalidSession() throws Exception {
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.openFile(SESSION_ID, mUri);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String
        .format(PreconditionMessage.ERR_UFS_MANAGER_OPERATION_INVALID_SESSION.toString(), "close"));
    manager.closeFile(INVALID_SESSION_ID, id);
  }

  /**
   * Tests getting an output stream for a valid file returns the correct output stream.
   */
  @Test
  public void getOutputStream() throws Exception {
    long id = mManager.createFile(SESSION_ID, mUri, Permission.defaults());
    Assert.assertEquals(mMockOutputStream, mManager.getOutputStream(id));
  }

  /**
   * Tests getting an output stream from an invalid file fails.
   */
  @Test
  public void getNonExistentOutputStream() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    mManager.getOutputStream(INVALID_FILE_ID);
  }

  /**
   * Tests getting an input stream to a valid file at the start returns the correct input stream.
   */
  @Test
  public void getInputStream() throws Exception {
    long position = 0L;
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    Mockito.when(mMockInputStream.read()).thenReturn(5);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(5, in.read());
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input stream.
   */
  @Test
  public void getInputStreamAtPosition() throws Exception {
    long position = FILE_LENGTH - 1;
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    Mockito.when(mMockInputStream.read()).thenReturn(5);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(5, in.read());
    Mockito.verify(mMockInputStream).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream to a valid file at a position returns the correct input stream
   * when using a UFS which supports opening at a position.
   */
  // TODO(calvin): Generalize this when openAtPosition is part of an interface
  @Test
  public void getInputStreamAtPositionOptimized() throws Exception {
    // Specifically testing S3A to validate the code path taken if the UFS implements the
    // openAtPosition api.
    S3AUnderFileSystem ufs = Mockito.mock(S3AUnderFileSystem.class);
    Mockito.when(ufs.openAtPosition(Mockito.anyString(), Mockito.anyLong())).thenReturn(
        mMockInputStream);
    Mockito.when(ufs.getFileSize(Mockito.anyString())).thenReturn(FILE_LENGTH);
    PowerMockito.mockStatic(UnderFileSystem.class);
    Mockito.when(UnderFileSystem.get(Mockito.anyString())).thenReturn(ufs);

    long position = FILE_LENGTH - 1;
    Mockito.when(ufs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    Mockito.when(mMockInputStream.read()).thenReturn(5);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(5, in.read());
    // Due to the optimization, we should not need to skip
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream returns the cached stream if the cached stream is positioned
   * correctly.
   */
  @Test
  public void getInputStreamAtPositionCache() throws Exception {
    long position = 0;
    long nextPosition = 100;
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    in.skip(nextPosition - position);
    InputStream in2 = mManager.getInputStreamAtPosition(id, nextPosition);
    Assert.assertEquals(in, in2);
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
    in.close();
  }

  /**
   * Tests getting an input stream returns a new stream if the cached stream is positioned beyond
   * the requested position.
   */
  @Test
  public void getInputStreamAtPositionInvalidCache() throws Exception {
    long position = 0;
    long nextPosition = 100;
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    in.skip(nextPosition - position);
    InputStream in2 = mManager.getInputStreamAtPosition(id, position);
    Assert.assertNotEquals(in, in2);
    in.close();
    in2.close();
  }

  /**
   * Tests getting an input stream at EOF returns null.
   */
  @Test
  public void getInputStreamAtEOF() throws Exception {
    long position = FILE_LENGTH;
    Mockito.when(mMockUfs.exists(mUri.toString())).thenReturn(true);
    long id = mManager.openFile(SESSION_ID, mUri);
    InputStream in = mManager.getInputStreamAtPosition(id, position);
    Assert.assertEquals(null, in);
    Mockito.verify(mMockInputStream, Mockito.never()).skip(position);
  }

  /**
   * Tests getting an input stream to an invalid file fails.
   */
  @Test
  public void getNonExistentInputStream() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(INVALID_FILE_ID));
    mManager.getInputStreamAtPosition(INVALID_FILE_ID, 0L);
  }

  /**
   * Tests that after cleaning a session the session may no longer be used to get an input stream.
   */
  @Test
  public void cleanSessions() throws Exception {
    String uniqPath2 = PathUtils.uniqPath();
    long sessionId2 = SESSION_ID + 1;
    long position = 0L;
    Mockito.when(mMockUfs.exists(Mockito.anyString())).thenReturn(true);
    long id1 = mManager.openFile(SESSION_ID, mUri);
    long id2 = mManager.openFile(sessionId2, new AlluxioURI(uniqPath2));
    // Both files should be accessible
    InputStream in1 = mManager.getInputStreamAtPosition(id1, position);
    InputStream in2 = mManager.getInputStreamAtPosition(id2, position);
    in1.close();
    in2.close();
    // Clean up second session
    mManager.cleanupSession(sessionId2);
    // First file should still be available
    in1 = mManager.getInputStreamAtPosition(id1, position);
    in1.close();
    // Second file should no longer be available
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(id2));
    mManager.getInputStreamAtPosition(id2, position);
  }
}
