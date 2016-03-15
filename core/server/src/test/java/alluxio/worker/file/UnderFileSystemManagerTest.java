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

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.OutputStream;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.class})
public class UnderFileSystemManagerTest {
  /**
   * Tests creating a file with the manager will call {@link UnderFileSystem#create}.
   */
  @Test
  public void createUfsFileTest() throws Exception {
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);
    Mockito.when(mockUfs.create(Mockito.anyString())).thenReturn(mockOutputStream);
    PowerMockito.mockStatic(UnderFileSystem.class);
    BDDMockito.given(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .willReturn(mockUfs);

    String testFile = "test";
    UnderFileSystemManager manager = new UnderFileSystemManager();
    manager.createFile(testFile);
    Mockito.verify(mockUfs).create(Mockito.contains(testFile));
  }

  /**
   * Tests completing a file with the manager will call {@link UnderFileSystem#rename}.
   */
  @Test
  public void completeUfsFileTest() throws Exception {
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);
    Mockito.when(mockUfs.create(Mockito.anyString())).thenReturn(mockOutputStream);
    PowerMockito.mockStatic(UnderFileSystem.class);
    BDDMockito.given(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .willReturn(mockUfs);

    String testFile = "test";
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(testFile);
    Mockito.verify(mockUfs).create(Mockito.contains(testFile));
    manager.completeFile(id);
    Mockito.verify(mockUfs).rename(Mockito.contains(testFile), Mockito.eq(testFile));
  }

  /**
   * Tests canceling a file with the manager will call {@link UnderFileSystem#delete}.
   */
  @Test
  public void cancelUfsFileTest() throws Exception {
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);
    Mockito.when(mockUfs.create(Mockito.anyString())).thenReturn(mockOutputStream);
    PowerMockito.mockStatic(UnderFileSystem.class);
    BDDMockito.given(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .willReturn(mockUfs);

    String testFile = "test";
    UnderFileSystemManager manager = new UnderFileSystemManager();
    long id = manager.createFile(testFile);
    Mockito.verify(mockUfs).create(Mockito.contains(testFile));
    manager.cancelFile(id);
    Mockito.verify(mockUfs).delete(Mockito.contains(testFile), Mockito.eq(false));
  }
}
