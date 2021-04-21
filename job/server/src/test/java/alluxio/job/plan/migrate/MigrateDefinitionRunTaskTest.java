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

package alluxio.job.plan.migrate;

import static junit.framework.TestCase.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.MockFileInStream;
import alluxio.client.file.MockFileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobServerContext;
import alluxio.job.RunTaskContext;
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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link MigrateDefinition#runTask(MigrateConfig, MigrateCommand, RunTaskContext)}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class MigrateDefinitionRunTaskTest {
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
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    when(mMockFileSystemContext.getClientContext())
        .thenReturn(ClientContext.create(conf));
    when(mMockFileSystemContext.getClusterConf()).thenReturn(conf);
    when(mMockFileSystemContext.getPathConf(any(AlluxioURI.class))).thenReturn(conf);
    mMockInStream = new MockFileInStream(TEST_SOURCE_CONTENTS);
    when(mMockFileSystem.openFile(eq(new AlluxioURI(TEST_SOURCE)),
        any(OpenFilePOptions.class))).thenReturn(mMockInStream);
    mMockOutStream = new MockFileOutStream(mMockFileSystemContext);
    when(mMockFileSystem.createFile(eq(new AlluxioURI(TEST_DESTINATION)),
        any(CreateFilePOptions.class))).thenReturn(mMockOutStream);
    mMockUfsManager = Mockito.mock(UfsManager.class);
  }

  /**
   * Tests that the bytes of the file to migrate are written to the destination stream.
   */
  @Test
  public void basicMigrateTest() throws Exception {
    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    Assert.assertArrayEquals(TEST_SOURCE_CONTENTS, mMockOutStream.toByteArray());
  }

  /**
   * Tests that when deleteSource is true,
   * the worker will delete the source directory if the directory contains nothing,
   * otherwise, the source directory is kept.
   */
  @Test
  public void deleteEmptySourceDir() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.newArrayList());
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
  }

  /**
   * Tests that when deleteSource is true,
   * the worker will delete the source directory if the directory contains only directories,
   * otherwise, the source directory is kept.
   */
  @Test
  public void deleteDirsOnlySourceDir() throws Exception {
    String inner = TEST_DIR + "/innerDir";
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR))).thenReturn(
        Lists.newArrayList(new URIStatus(new FileInfo().setPath(inner).setFolder(true))));
    when(mMockFileSystem.listStatus(new AlluxioURI(inner)))
        .thenReturn(Lists.newArrayList());
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
  }

  /**
   * Tests that the worker will not delete the source directory if the directory still contains
   * files because this means not all files have been migrated.
   */
  @Test
  public void dontDeleteNonEmptySourceTest() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.newArrayList(new URIStatus(new FileInfo())));
    runTask(TEST_DIR, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    verify(mMockFileSystem, never()).delete(eq(new AlluxioURI(TEST_DIR)),
        any(DeletePOptions.class));
  }

  /**
   * Tests that the worker writes with the specified write type.
   */
  @Test
  public void writeTypeTest() throws Exception {
    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.CACHE_THROUGH);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DESTINATION)), Matchers
        .eq(CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build()));

    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.MUST_CACHE);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DESTINATION)), Matchers
        .eq(CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build()));
  }

  /**
   * Tests the edge case where performing an WriteType.AsyncThrough with deleteSource of a
   * persisted file writes the move synchronously.
   */
  @Test
  public void writeTypeAsyncThroughPersistedTest() throws Exception {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPersisted(true);
    when(mMockFileSystem.getStatus(eq(new AlluxioURI(TEST_SOURCE))))
        .thenReturn(new URIStatus(fileInfo));

    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.ASYNC_THROUGH);
  }

  @Test
  public void overwriteTest() throws Exception {
    final AtomicBoolean deleteCalled = new AtomicBoolean(false);

    when(mMockFileSystem.createFile(eq(new AlluxioURI(TEST_DESTINATION)), any()))
        .thenAnswer((invocation) -> {
          if (deleteCalled.get()) {
            return mMockOutStream;
          }
          throw new FileAlreadyExistsException("already exists");
        });

    doAnswer((invocation) -> {
      if (deleteCalled.get()) {
        throw new FileDoesNotExistException("doesn't exist");
      }
      deleteCalled.set(true);
      return null;
    }).when(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DESTINATION)));

    try {
      runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH, false);
      fail();
    } catch (FileAlreadyExistsException e) {
      // expected
    }

    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH, true);
  }

  /**
   * Runs the task.
   *
   * @param configSource {@link MigrateConfig} source
   * @param commandSource {@link MigrateCommand} source
   * @param commandDestination {@link MigrateCommand} destination
   * @param writeType {@link MigrateConfig} writeType
   */
  private void runTask(String configSource, String commandSource, String commandDestination,
      WriteType writeType) throws Exception {
    runTask(configSource, commandSource, commandDestination, writeType, false);
  }

  /**
   * Runs the task.
   *
   * @param configSource {@link MigrateConfig} source
   * @param commandSource {@link MigrateCommand} source
   * @param commandDestination {@link MigrateCommand} destination
   * @param writeType {@link MigrateConfig} writeType
   * @param overwrite (@link MigrateConfig} overwrite
   */
  private void runTask(String configSource, String commandSource, String commandDestination,
                       WriteType writeType, boolean overwrite) throws Exception {
    new MigrateDefinition().runTask(
        new MigrateConfig(configSource, "", writeType.toString(), overwrite),
        new MigrateCommand(commandSource, commandDestination),
        new RunTaskContext(1, 1,
            new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));
  }
}
