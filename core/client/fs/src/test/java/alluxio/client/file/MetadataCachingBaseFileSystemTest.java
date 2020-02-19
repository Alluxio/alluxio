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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.resource.CloseableResource;
import alluxio.wire.FileInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class})
public class MetadataCachingBaseFileSystemTest {
  private static final AlluxioURI DIR = new AlluxioURI("/dir");
  private static final AlluxioURI FILE = new AlluxioURI("/dir/file");
  private static final ListStatusPOptions LIST_STATUS_OPTIONS =
      ListStatusPOptions.getDefaultInstance();
  private static final URIStatus FILE_STATUS = new URIStatus(
      new FileInfo().setPath(FILE.getPath()).setCompleted(true));

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mFileContext;
  private ClientContext mClientContext;
  private FileSystemMasterClient mFileSystemMasterClient;
  private MetadataCachingBaseFileSystem mFs;

  @Before
  public void before() throws Exception {
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    when(mFileContext.acquireMasterClientResource()).thenReturn(
        new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClient) {
          @Override
          public void close() {
            // Noop.
          }
        });
    when(mFileSystemMasterClient.listStatus(eq(DIR), any(ListStatusPOptions.class)))
        .thenReturn(Arrays.asList(FILE_STATUS));
    when(mFileSystemMasterClient.getStatus(eq(FILE), any(GetStatusPOptions.class)))
        .thenReturn(FILE_STATUS);
    when(mFileContext.getClientContext()).thenReturn(mClientContext);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mFs = new MetadataCachingBaseFileSystem(mFileContext);
  }

  @After
  public void after() {
    mConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void getStatus() throws Exception {
    mFs.getStatus(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
    // The following getStatus gets from cache, so no RPC will be made.
    mFs.getStatus(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
  }

  @Test
  public void listStatus() throws Exception {
    List<URIStatus> expectedStatuses = mFs.listStatus(DIR);
    verifyListStatusThroughRPC(DIR, 1);
    // List status has cached the file status, so no RPC will be made.
    mFs.getStatus(FILE);
    verifyGetStatusThroughRPC(FILE, 0);
    List<URIStatus> gotStatuses = mFs.listStatus(DIR);
    // List status results have been cached, so listStatus RPC was only called once
    // at the beginning of the method.
    verifyListStatusThroughRPC(DIR, 1);
    assertEquals(expectedStatuses, gotStatuses);
  }

  @Test
  public void listStatusRecursive() throws Exception {
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    verifyListStatusThroughRPC(DIR, 1);
    mFs.listStatus(DIR, LIST_STATUS_OPTIONS.toBuilder().setRecursive(true).build());
    verifyListStatusThroughRPC(DIR, 2);
  }

  @Test
  public void openFile() throws Exception {
    mFs.openFile(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
    mFs.openFile(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
  }

  @Test
  public void getBlockLocations() throws Exception {
    mFs.getBlockLocations(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
    mFs.getBlockLocations(FILE);
    verifyGetStatusThroughRPC(FILE, 1);
  }

  private void verifyGetStatusThroughRPC(AlluxioURI path, int totalTimes) throws Exception {
    verify(mFileSystemMasterClient, times(totalTimes))
        .getStatus(eq(path), any(GetStatusPOptions.class));
  }

  private void verifyListStatusThroughRPC(AlluxioURI path, int totalTimes) throws Exception {
    verify(mFileSystemMasterClient, times(totalTimes))
        .listStatus(eq(path), any(ListStatusPOptions.class));
  }
}
