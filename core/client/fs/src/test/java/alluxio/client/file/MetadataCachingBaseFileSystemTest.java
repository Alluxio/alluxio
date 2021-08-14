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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.resource.CloseableResource;
import alluxio.wire.FileInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public class MetadataCachingBaseFileSystemTest {
  private static final AlluxioURI DIR = new AlluxioURI("/dir");
  private static final AlluxioURI FILE = new AlluxioURI("/dir/file");
  private static final AlluxioURI NOT_EXIST_FILE = new AlluxioURI("/dir/not_exist_file");
  private static final ListStatusPOptions LIST_STATUS_OPTIONS =
      ListStatusPOptions.getDefaultInstance();
  private static final URIStatus FILE_STATUS =
      new URIStatus(new FileInfo().setPath(FILE.getPath()).setCompleted(true));

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mFileContext;
  private ClientContext mClientContext;
  private RpcCountingFileSystemMasterClient mFileSystemMasterClient;
  private Map<AlluxioURI, URIStatus> mFileStatusMap;
  private MetadataCachingBaseFileSystem mFs;

  @Before
  public void before() throws Exception {
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = new RpcCountingFileSystemMasterClient();
    when(mFileContext.acquireMasterClientResource())
        .thenReturn(new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClient) {
          @Override
          public void close() {
            // Noop.
          }
        });
    when(mFileContext.getClientContext()).thenReturn(mClientContext);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mFs = new MetadataCachingBaseFileSystem(mFileContext);
    mFileStatusMap = new HashMap<>();
  }

  @After
  public void after() {
    mConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void getStatus() throws Exception {
    mFs.getStatus(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
  }

  @Test
  public void iterateStatus() throws Exception {
    List<URIStatus> expectedStatuses = new ArrayList<>();
    mFs.iterateStatus(DIR, expectedStatuses::add);
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    // List status has cached the file status, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(0, mFileSystemMasterClient.getStatusRpcCount(FILE));
    List<URIStatus> gotStatuses = new ArrayList<>();
    mFs.iterateStatus(DIR, gotStatuses::add);
    // List status results have been cached, so listStatus RPC was only called once
    // at the beginning of the method.
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    assertEquals(expectedStatuses, gotStatuses);
  }

  @Test
  public void iterateStatusRecursive() throws Exception {
    mFs.iterateStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build(), ignored -> {
    });
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    mFs.iterateStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build(), ignored -> {
    });
    assertEquals(2, mFileSystemMasterClient.listStatusRpcCount(DIR));
  }

  @Test
  public void listStatus() throws Exception {
    List<URIStatus> expectedStatuses = mFs.listStatus(DIR);
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    // List status has cached the file status, so no RPC will be made.
    mFs.getStatus(FILE);
    assertEquals(0, mFileSystemMasterClient.getStatusRpcCount(FILE));
    List<URIStatus> gotStatuses = mFs.listStatus(DIR);
    // List status results have been cached, so listStatus RPC was only called once
    // at the beginning of the method.
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    assertEquals(expectedStatuses, gotStatuses);
  }

  @Test
  public void listStatusRecursive() throws Exception {
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    assertEquals(1, mFileSystemMasterClient.listStatusRpcCount(DIR));
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    assertEquals(2, mFileSystemMasterClient.listStatusRpcCount(DIR));
  }

  @Test
  public void openFile() throws Exception {
    mFs.openFile(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
    mFs.openFile(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
  }

  @Test
  public void getBlockLocations() throws Exception {
    mFs.getBlockLocations(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
    mFs.getBlockLocations(FILE);
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(FILE));
  }

  @Test
  public void getNoneExistStatus() throws Exception {
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void createNoneExistFile() throws Exception {
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    assertEquals(2, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void createNoneExistDirectory() throws Exception {
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(1, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.createDirectory(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    assertEquals(2, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void createAndDelete() throws Exception {
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    mFs.delete(NOT_EXIST_FILE);
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(2, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
  }

  @Test
  public void createAndRename() throws Exception {
    mFs.createFile(NOT_EXIST_FILE);
    mFs.getStatus(NOT_EXIST_FILE);
    mFs.rename(NOT_EXIST_FILE, new AlluxioURI(NOT_EXIST_FILE.getPath() + ".rename"));
    try {
      mFs.getStatus(NOT_EXIST_FILE);
      Assert.fail("Failed while getStatus for a non-exist path.");
    } catch (FileDoesNotExistException e) {
      // expected exception thrown. test passes
    }
    assertEquals(2, mFileSystemMasterClient.getStatusRpcCount(NOT_EXIST_FILE));
  }

  class RpcCountingFileSystemMasterClient extends MockFileSystemMasterClient {
    RpcCountingFileSystemMasterClient() {
    }

    private Map<AlluxioURI, Integer> mGetStatusCount = new HashMap<>();
    private Map<AlluxioURI, Integer> mListStatusCount = new HashMap<>();

    int getStatusRpcCount(AlluxioURI uri) {
      return mGetStatusCount.getOrDefault(uri, 0);
    }

    int listStatusRpcCount(AlluxioURI uri) {
      return mListStatusCount.getOrDefault(uri, 0);
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
        throws AlluxioStatusException {
      mGetStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      if (path.toString().equals(FILE_STATUS.getPath())) {
        return FILE_STATUS;
      }
      if (mFileStatusMap.containsKey(path)) {
        return mFileStatusMap.get(path);
      }
      throw new NotFoundException("Path \"" + path.getPath() + "\" does not exist.");
    }

    @Override
    public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
        Consumer<? super URIStatus> action) throws AlluxioStatusException {
      mListStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      action.accept(FILE_STATUS);
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
        throws AlluxioStatusException {
      mListStatusCount.compute(path, (k, v) -> v == null ? 1 : v + 1);
      return Arrays.asList(FILE_STATUS);
    }

    @Override
    public URIStatus createFile(AlluxioURI path, CreateFilePOptions options)
        throws AlluxioStatusException {
      URIStatus status = new URIStatus(
          new FileInfo()
              .setPath(NOT_EXIST_FILE.getPath())
              .setCompleted(true));
      mFileStatusMap.put(path, status);
      return status;
    }

    @Override
    public void createDirectory(AlluxioURI path,
        CreateDirectoryPOptions options) throws AlluxioStatusException {
      URIStatus status = new URIStatus(
          new FileInfo()
              .setPath(NOT_EXIST_FILE.getPath())
              .setCompleted(true));
      mFileStatusMap.put(path, status);
    }

    @Override
    public void delete(AlluxioURI path, DeletePOptions options)
        throws AlluxioStatusException {
      mFileStatusMap.remove(path);
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
        throws AlluxioStatusException {
      mFileStatusMap.put(dst, mFileStatusMap.remove(src));
    }
  }
}
