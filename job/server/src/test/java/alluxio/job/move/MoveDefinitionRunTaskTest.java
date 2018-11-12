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

package alluxio.job.move;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.*;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobWorkerContext;
import alluxio.underfs.UfsManager;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

/**
 * Unit tests for {@link MoveDefinition#runTask(MoveConfig, ArrayList, JobWorkerContext)}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class MoveDefinitionRunTaskTest {
  private static final String TEST_DIR = "/DIR";
  private static final String TEST_SOURCE = "/DIR/TEST_SOURCE";
  private static final String TEST_DESTINATION = "/DIR/TEST_DESTINATION";
  private static final byte[] TEST_SOURCE_CONTENTS = BufferUtils.getIncreasingByteArray(100);

  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private MockFileInStream mMockInStream;
  private MockFileOutStream mMockOutStream;
  private UfsManager mMockUfsManager;

  @Before
  public void before() throws Exception {
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockInStream = new MockFileInStream(FileSystemContext.get(), TEST_SOURCE_CONTENTS);
    when(mMockFileSystem.openFile(new AlluxioURI(TEST_SOURCE))).thenReturn(mMockInStream);
    mMockOutStream = new MockFileOutStream();
    when(mMockFileSystem.createFile(eq(new AlluxioURI(TEST_DESTINATION)),
        any(CreateFilePOptions.class))).thenReturn(mMockOutStream);
    mMockUfsManager = Mockito.mock(UfsManager.class);
  }

  /**
   * Tests that the bytes of the file to move are written to the destination stream.
   */
  @Test
  public void basicMoveTest() throws Exception {
    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    Assert.assertArrayEquals(TEST_SOURCE_CONTENTS, mMockOutStream.toByteArray());
    verify(mMockFileSystem).delete(new AlluxioURI(TEST_SOURCE));
  }

  /**
   * Tests that the worker will delete the source directory if the directory contains nothing.
   */
  @Test
  public void deleteEmptySourceDir() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.<URIStatus>newArrayList());
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeletePOptions.class));
  }

  /**
   * Tests that the worker will delete the source directory if the directory contains only
   * directories.
   */
  @Test
  public void deleteDirsOnlySourceDir() throws Exception {
    String inner = TEST_DIR + "/innerDir";
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR))).thenReturn(
        Lists.newArrayList(new URIStatus(new FileInfo().setPath(inner).setFolder(true))));
    when(mMockFileSystem.listStatus(new AlluxioURI(inner)))
        .thenReturn(Lists.<URIStatus>newArrayList());
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeletePOptions.class));
  }

  /**
   * Tests that the worker will not delete the source directory if the directory still contains
   * files.
   */
  @Test
  public void dontDeleteNonEmptySourceTest() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.newArrayList(new URIStatus(new FileInfo())));
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    verify(mMockFileSystem, times(0)).delete(eq(new AlluxioURI(TEST_DIR)),
        any(DeletePOptions.class));
  }

  /**
   * Tests that the worker writes with the specified write type.
   */
  @Test
  public void writeTypeTest() throws Exception {
    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.CACHE_THROUGH);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DESTINATION)),
        Matchers.eq(FileSystemClientOptions.getCreateFileOptions().toBuilder()
            .setWriteType(WritePType.WRITE_CACHE_THROUGH)).build());

    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.MUST_CACHE);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DESTINATION)),
        Matchers.eq(FileSystemClientOptions.getCreateFileOptions().toBuilder()
            .setWriteType(WritePType.WRITE_MUST_CACHE)).build());
  }

  /**
   * Runs the task.
   *
   * @param configSource {@link MoveConfig} source
   * @param commandSource {@link MoveCommand} source
   * @param commandDestination {@link MoveCommand} destination
   * @param writeType {@link MoveConfig} writeType
   */
  private void runTask(String configSource, String commandSource, String commandDestination,
      WriteType writeType) throws Exception {
    new MoveDefinition(mMockFileSystemContext, mMockFileSystem).runTask(
        new MoveConfig(configSource, "", writeType.toString(), false),
        Lists.newArrayList(new MoveCommand(commandSource, commandDestination)),
        new JobWorkerContext(1, 1, mMockUfsManager));
  }
}
