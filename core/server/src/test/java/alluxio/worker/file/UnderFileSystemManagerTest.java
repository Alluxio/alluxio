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

import java.io.OutputStream;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.class})
public final class UnderFileSystemManagerTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /** The mock under file system client. */
  private UnderFileSystem mMockUfs;

  @Before
  public void before() throws Exception {
    mMockUfs = Mockito.mock(UnderFileSystem.class);
    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);
    Mockito.when(mMockUfs.create(Mockito.anyString())).thenReturn(mockOutputStream);
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
    manager.createFile(new AlluxioURI(uniqPath));
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
    manager.createFile(new AlluxioURI(uniqPath));
  }

  /**
   * Tests completing a file with the manager will call {@link UnderFileSystem#rename}.
   */
  @Test
  public void completeUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.completeFile(id);
    Mockito.verify(mMockUfs).rename(Mockito.contains(uniqPath), Mockito.eq(uniqPath));
  }

  /**
   * Tests completing a non existent file throws the correct exception.
   */
  @Test
  public void completeNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.completeFile(-1L);
  }

  /**
   * Tests canceling a file with the manager will call {@link UnderFileSystem#delete}.
   */
  @Test
  public void cancelUfsFileTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(new AlluxioURI(uniqPath));
    Mockito.verify(mMockUfs).create(Mockito.contains(uniqPath));
    manager.cancelFile(id);
    Mockito.verify(mMockUfs).delete(Mockito.contains(uniqPath), Mockito.eq(false));
  }

  /**
   * Tests canceling a non existent file throws the correct exception.
   */
  @Test
  public void cancelNonExistentUfsFileTest() throws Exception {
    UnderFileSystemManager manager = new UnderFileSystemManager();
    mThrown.expect(FileDoesNotExistException.class);
    manager.cancelFile(-1L);
  }
}
