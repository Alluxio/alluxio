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

package alluxio.job.migrate;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Unit tests for {@link MigrateDefinition#runTask(MigrateConfig, ArrayList, RunTaskContext)}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
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

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { {true}, {false} });
  }

  @Parameter
  public boolean mDeleteSource;

  @Before
  public void before() throws Exception {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    when(mMockFileSystemContext.getClientContext())
        .thenReturn(ClientContext.create(conf));
    when(mMockFileSystemContext.getConf())
        .thenReturn(conf);
    mMockInStream = new MockFileInStream(mMockFileSystemContext, TEST_SOURCE_CONTENTS, conf);
    when(mMockFileSystem.openFile(new AlluxioURI(TEST_SOURCE))).thenReturn(mMockInStream);
    mMockOutStream = new MockFileOutStream(mMockFileSystemContext);
    when(mMockFileSystem.createFile(eq(new AlluxioURI(TEST_DESTINATION)),
        any(CreateFilePOptions.class))).thenReturn(mMockOutStream);
    mMockUfsManager = Mockito.mock(UfsManager.class);
  }

  /**
   * Tests that the bytes of the file to migrate are written to the destination stream.
   * When deleteSource is true, the source is deleted, otherwise, it is kept.
   */
  @Test
  public void basicMigrateTest() throws Exception {
    runTask(TEST_SOURCE, TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);
    Assert.assertArrayEquals(TEST_SOURCE_CONTENTS, mMockOutStream.toByteArray());
    if (mDeleteSource) {
      verify(mMockFileSystem).delete(new AlluxioURI(TEST_SOURCE));
    } else {
      verify(mMockFileSystem, never()).delete(new AlluxioURI(TEST_SOURCE));
    }
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
    if (mDeleteSource) {
      verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeletePOptions.class));
    } else {
      verify(mMockFileSystem, never()).delete(new AlluxioURI(TEST_DIR));
    }
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
    if (mDeleteSource) {
      verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeletePOptions.class));
    } else {
      verify(mMockFileSystem, never()).delete(new AlluxioURI(TEST_DIR));
    }
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
   * Runs the task.
   *
   * @param configSource {@link MigrateConfig} source
   * @param commandSource {@link MigrateCommand} source
   * @param commandDestination {@link MigrateCommand} destination
   * @param writeType {@link MigrateConfig} writeType
   */
  private void runTask(String configSource, String commandSource, String commandDestination,
      WriteType writeType) throws Exception {
    new MigrateDefinition().runTask(
        new MigrateConfig(configSource, "", writeType.toString(), false, mDeleteSource),
        Lists.newArrayList(new MigrateCommand(commandSource, commandDestination)),
        new RunTaskContext(1, 1,
            new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));
  }
}
