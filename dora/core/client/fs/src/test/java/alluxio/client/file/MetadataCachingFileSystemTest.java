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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.wire.FileInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public class MetadataCachingFileSystemTest {
  private static final AlluxioURI DIR = new AlluxioURI("/dir");
  private static final AlluxioURI FILE = new AlluxioURI("/dir/file");
  private static final AlluxioURI NOT_EXIST_FILE = new AlluxioURI("/dir/not_exist_file");
  private static final ListStatusPOptions LIST_STATUS_OPTIONS =
      ListStatusPOptions.getDefaultInstance();
  private static final URIStatus FILE_STATUS =
      new URIStatus(new FileInfo().setPath(FILE.getPath()).setCompleted(true));

  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private FileSystemContext mFileContext;
  private ClientContext mClientContext;
  private Map<AlluxioURI, URIStatus> mFileStatusMap;
  private MetadataCachingFileSystem mFs;
  private RpcCountingUfsBaseFileSystem mRpcCountingFs;

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE, 1000, Source.RUNTIME);
    // Avoid async update file access time to call getStatus to mess up the test results
    mConf.set(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED, true);
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    when(mFileContext.getClientContext()).thenReturn(mClientContext);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    // This is intentionally an empty mock
    // If RpcCountingUfsBaseFileSystem fails to serve a method, the empty mock will err
    UfsBaseFileSystem delegatedFs = mock(UfsBaseFileSystem.class);
    RpcCountingUfsBaseFileSystem fs = new RpcCountingUfsBaseFileSystem(delegatedFs, mFileContext);
    mRpcCountingFs = fs;
    mFs = new MetadataCachingFileSystem(fs, mFileContext);
    mFileStatusMap = new HashMap<>();
  }

  @After
  public void after() {
    mConf = Configuration.copyGlobal();
  }

  @Test
  public void getStatus() throws Exception {
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
  }

  @Test
  public void iterateStatus() throws Exception {
    List<URIStatus> expectedStatuses = new ArrayList<>();
    mFs.iterateStatus(DIR, expectedStatuses::add);
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    // List status has cached the file status, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(0, mRpcCountingFs.getStatusRpcCount(FILE));
    List<URIStatus> gotStatuses = new ArrayList<>();
    mFs.iterateStatus(DIR, gotStatuses::add);
    // List status results have been cached, so listStatus RPC was only called once
    // at the beginning of the method.
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    assertEquals(expectedStatuses, gotStatuses);
  }

  @Test
  public void iterateStatusRecursive() throws Exception {
    mFs.iterateStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build(), ignored -> {
    });
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    mFs.iterateStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build(), ignored -> {
    });
    assertEquals(2, mRpcCountingFs.listStatusRpcCount(DIR));
  }

  @Test
  public void listStatus() throws Exception {
    List<URIStatus> expectedStatuses = mFs.listStatus(DIR);
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    // List status has cached the file status, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(0, mRpcCountingFs.getStatusRpcCount(FILE));
    List<URIStatus> gotStatuses = mFs.listStatus(DIR);
    // List status results have been cached, so listStatus RPC was only called once
    // at the beginning of the method.
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    assertEquals(expectedStatuses, gotStatuses);
  }

  @Test
  public void listStatusRecursive() throws Exception {
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    assertEquals(2, mRpcCountingFs.listStatusRpcCount(DIR));
  }

  @Test
  public void openFile() throws Exception {
    mFs.openFile(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.openFile(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
  }

  @Ignore
  @Test
  @DoraTestTodoItem(owner = "hua", action = DoraTestTodoItem.Action.FIX,
        comment = "UfsBaseFileSystem needs to support getBlockLocations for the application."
            + "The concept of block here is HDFS block, instead of the deprecated Alluxio block.")
  public void getBlockLocations() throws Exception {
    mFs.getBlockLocations(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.getBlockLocations(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
  }

  @Test
  public void getNoneExistStatus() throws Exception {
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException  e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Deprecated
  public void createNoneExistFile() throws Exception {
    // TODO(Hua): now we don't need to request master when creating file.
    // This method need to be reimplemented.
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    assertEquals(2, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void createNoneExistDirectory() throws Exception {
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.createDirectory(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    assertEquals(2, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Deprecated
  public void createAndDelete() throws Exception {
    // TODO(Hua): now we don't need to request master when creating file.
    // This method need to be reimplemented.
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    mFs.delete(NOT_EXIST_FILE);
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(2, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Deprecated
  public void createAndRename() throws Exception {
    // TODO(Hua): now we don't need to request master when creating file.
    // This method need to be reimplemented.
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    mFs.rename(NOT_EXIST_FILE, new AlluxioURI(NOT_EXIST_FILE.getPath() + ".rename"));
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(2, mRpcCountingFs.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void dropMetadataCacheFile() throws Exception {
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.dropMetadataCache(FILE);
    mFs.getStatus(FILE);
    assertEquals(2, mRpcCountingFs.getStatusRpcCount(FILE));
  }

  @Test
  public void dropMetadataCacheDir() throws Exception {
    mFs.listStatus(DIR);
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    mFs.dropMetadataCache(DIR);
    mFs.listStatus(DIR);
    assertEquals(2, mRpcCountingFs.listStatusRpcCount(DIR));
  }

  @Test
  public void dropMetadataCacheDirFile() throws Exception {
    mFs.listStatus(DIR);
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.listStatusRpcCount(DIR));
    assertEquals(0, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.dropMetadataCache(FILE);
    mFs.getStatus(FILE);
    assertEquals(1, mRpcCountingFs.getStatusRpcCount(FILE));
    mFs.listStatus(DIR);
    assertEquals(2, mRpcCountingFs.listStatusRpcCount(DIR));
  }

  class RpcCountingUfsBaseFileSystem extends DelegatingFileSystem {
    private Map<AlluxioURI, Integer> mGetStatusCount = new HashMap<>();
    private Map<AlluxioURI, Integer> mListStatusCount = new HashMap<>();
    private FileSystemContext mContext;

    public RpcCountingUfsBaseFileSystem(FileSystem fs, FileSystemContext context) {
      super(fs);
      mContext = context;
    }

    int getStatusRpcCount(AlluxioURI uri) {
      return mGetStatusCount.getOrDefault(uri, 0);
    }

    int listStatusRpcCount(AlluxioURI uri) {
      return mListStatusCount.getOrDefault(uri, 0);
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, final GetStatusPOptions options)
        throws FileDoesNotExistException {
      mGetStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      if (path.toString().equals(FILE_STATUS.getPath())) {
        return FILE_STATUS;
      }
      if (mFileStatusMap.containsKey(path)) {
        return mFileStatusMap.get(path);
      }
      throw new FileDoesNotExistException("Path \"" + path.getPath() + "\" does not exist.");
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, final ListStatusPOptions options) {
      mListStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      return Arrays.asList(FILE_STATUS);
    }

    @Override
    public void iterateStatus(AlluxioURI path, final ListStatusPOptions options,
                              Consumer<? super URIStatus> action) {
      mListStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      action.accept(FILE_STATUS);
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options) {
      URIStatus status = new URIStatus(
          new FileInfo()
              .setPath(NOT_EXIST_FILE.getPath())
              .setCompleted(true));
      mFileStatusMap.put(path, status);
      try {
        return new MockFileOutStream(mContext);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options) {
      URIStatus status = new URIStatus(
          new FileInfo()
              .setPath(NOT_EXIST_FILE.getPath())
              .setCompleted(true));
      mFileStatusMap.put(path, status);
    }

    @Override
    public void delete(AlluxioURI path, DeletePOptions options) {
      mFileStatusMap.remove(path);
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options) {
      mFileStatusMap.put(dst, mFileStatusMap.remove(src));
    }
  }
}
