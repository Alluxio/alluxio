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

package alluxio.fuse;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.resource.CloseableResource;
import alluxio.wire.FileInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Isolation tests for {@link AlluxioFuseClientToolsTest}.
 */
@RunWith(PowerMockRunner.class)
public class AlluxioFuseClientToolsTest {
  private AlluxioFuseClientTools mAlluxioFuseClientTools;
  private Map<AlluxioURI, URIStatus> mFileStatusMap;
  private FileSystem mFileSystem;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemMasterClient mFileSystemMasterClient;
  private FileSystemContext mFileContext;
  private ClientContext mClientContext;

  private static final AlluxioURI DIR = new AlluxioURI("/dir");
  private static final AlluxioURI FILE = new AlluxioURI("/dir/file");
  private static final URIStatus DIR_STATUS =
      new URIStatus(new FileInfo().setPath(DIR.getPath()).setCompleted(true));
  private static final URIStatus FILE_STATUS =
      new URIStatus(new FileInfo().setPath(FILE.getPath()).setCompleted(true));
  private static final URIStatus NOT_FOUND_STATUS =
      new URIStatus(new FileInfo().setCompleted(true));

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_METADATA_CACHE_ENABLED, true);
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = new GetStatusFileSystemMasterClient();
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
    mFileSystem = new MetadataCachingBaseFileSystem(mFileContext);
    mAlluxioFuseClientTools = new AlluxioFuseClientTools(mFileSystem, mConf);
    mFileStatusMap = new HashMap<>();
    mFileStatusMap.put(FILE, FILE_STATUS);
    mFileStatusMap.put(DIR, DIR_STATUS);
    // Here metadata cache will have two contents.
    mFileSystem.getStatus(FILE);
    mFileSystem.getStatus(DIR);
    // Remove from map, so the result will get from cache.
    mFileStatusMap.remove(FILE);
    mFileStatusMap.remove(DIR);
  }

  @Test
  public void runGetMetadataCacheSizeCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/.alluxiocli.metadatacache.size");
    URIStatus status = mAlluxioFuseClientTools.runCommand(reservedPath);
    assertEquals(2, status.getFileInfo().getLength());
  }

  @Test
  public void runDropMetadataCacheCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.drop");
    // Drop the specific path cache, the other one will remain.
    mAlluxioFuseClientTools.runCommand(reservedPath);
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(DIR));
    assertEquals(FILE_STATUS, mFileSystem.getStatus(FILE));
  }

  @Test
  public void runDropAllMetadataCacheCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.dropAll");
    // All cache will be dropped.
    mAlluxioFuseClientTools.runCommand(reservedPath);
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(DIR));
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(FILE));
  }

  @Test
  public  void runNoneExistCommand() {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.None");
    assertEquals(null, mAlluxioFuseClientTools.runCommand(reservedPath));
  }

  class GetStatusFileSystemMasterClient extends MockFileSystemMasterClient {
    GetStatusFileSystemMasterClient() {
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
        throws AlluxioStatusException {
      if (mFileStatusMap.containsKey(path)) {
        return mFileStatusMap.get(path);
      }
      return NOT_FOUND_STATUS;
    }
  }
}
