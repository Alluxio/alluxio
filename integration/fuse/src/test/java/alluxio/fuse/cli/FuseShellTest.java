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

package alluxio.fuse.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.cli.FuseShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.metadata.FuseURIStatus;
import alluxio.grpc.GetStatusPOptions;
import alluxio.resource.CloseableResource;
import alluxio.wire.FileInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Isolation tests for {@link FuseShell}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public class FuseShellTest {
  private FuseShell mFuseShell;
  private Map<AlluxioURI, URIStatus> mFileStatusMap;
  private FileSystem mFileSystem;
  private final InstancedConfiguration mConf = Configuration.copyGlobal();
  private FileSystemMasterClient mFileSystemMasterClient;

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
    ClientContext clientContext = ClientContext.create(mConf);
    FileSystemContext fileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = new GetStatusFileSystemMasterClient();
    when(fileContext.acquireMasterClientResource())
        .thenReturn(new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClient) {
          @Override
          public void closeResource() {
            // Noop.
          }
        });
    when(fileContext.getClientContext()).thenReturn(clientContext);
    when(fileContext.getClusterConf()).thenReturn(mConf);
    when(fileContext.getPathConf(any())).thenReturn(mConf);
    when(fileContext.getUriValidationEnabled()).thenReturn(true);
    mFileSystem = new MetadataCachingBaseFileSystem(fileContext);
    mFuseShell = new FuseShell(mFileSystem, AlluxioFuseFileSystemOpts.create(mConf));
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
  public void validateAndParseSpecialURITest() {
    AlluxioURI reservedPath = new AlluxioURI("/.alluxiocli.metadatacache.size");
    boolean isSpecialPath = mFuseShell.isSpecialCommand(reservedPath);
    assertTrue(isSpecialPath);
  }

  @Test
  public void validateAndParseNormalURITest() {
    AlluxioURI reservedPath = new AlluxioURI("/test/dir/file");
    boolean isSpecialPath = mFuseShell.isSpecialCommand(reservedPath);
    assertFalse(isSpecialPath);
  }

  @Test(expected = InvalidArgumentException.class)
  public  void runMetadataCacheCommandWhenSpecialCommandDisable() throws InvalidArgumentException {
    mConf.set(PropertyKey.USER_METADATA_CACHE_ENABLED, false);
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.drop");
    new FuseShell(mFileSystem, AlluxioFuseFileSystemOpts.create(mConf)).runCommand(reservedPath);
  }

  @Test(expected = InvalidArgumentException.class)
  public  void runNoneExistCommand() throws InvalidArgumentException {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.None.subcommand");
    mFuseShell.runCommand(reservedPath);
  }

  @Test(expected = InvalidArgumentException.class)
  public  void runNoneExistSubCommand() throws InvalidArgumentException {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.None");
    mFuseShell.runCommand(reservedPath);
  }

  @Test
  public void runGetMetadataCacheSizeCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/.alluxiocli.metadatacache.size");
    FuseURIStatus status = mFuseShell.runCommand(reservedPath);
    assertEquals(2, status.getLength());
  }

  @Test
  public void runDropMetadataCacheCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.drop");
    // Drop the specific path cache, the other one will remain.
    mFuseShell.runCommand(reservedPath);
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(DIR));
    assertEquals(FILE_STATUS, mFileSystem.getStatus(FILE));
  }

  @Test
  public void runDropAllMetadataCacheCommand() throws Exception {
    AlluxioURI reservedPath = new AlluxioURI("/dir/.alluxiocli.metadatacache.dropAll");
    // All cache will be dropped.
    mFuseShell.runCommand(reservedPath);
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(DIR));
    assertEquals(NOT_FOUND_STATUS, mFileSystem.getStatus(FILE));
  }

  class GetStatusFileSystemMasterClient extends MockFuseFileSystemMasterClient {
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
