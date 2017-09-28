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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link FileSystemMasterClientRestServiceHandler}.
 */
public final class FileSystemMasterClientRestApiTest extends RestApiTest {
  private static final GetStatusOptions GET_STATUS_OPTIONS = GetStatusOptions.defaults();

  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = FileSystemMasterClientRestServiceHandler.SERVICE_PREFIX;
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_NAME), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_VERSION), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void completeFile() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    long id = mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("ufsLength", "1");
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.COMPLETE_FILE), params,
        HttpMethod.POST, null).run();
    Assert.assertTrue(mFileSystemMaster.getFileInfo(id).isCompleted());
  }

  @Test
  public void createDirectory() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("allowExists", "false");
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_DIRECTORY), params,
        HttpMethod.POST, null).run();

    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void createFile() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("blockSizeBytes", "1");
    params.put("blockSizeBytes", "1");
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_FILE), params, HttpMethod.POST,
        null).run();
    Assert.assertFalse(mFileSystemMaster.getFileInfo(uri, GET_STATUS_OPTIONS).isCompleted());
  }

  @Test
  public void getNewBlockIdForFile() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_NEW_BLOCK_ID_FOR_FILE), params,
        HttpMethod.POST, null).call();
  }

  @Test
  public void getStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    String result = new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_STATUS), params, HttpMethod.GET,
        null).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    Assert.assertEquals(uri.getPath(), fileInfo.getPath());
    Assert.assertEquals(0, fileInfo.getLength());
  }

  @Test
  public void free() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    // Mark the file as persisted so the "free" works.
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults().setPersisted(true));
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("recursive", "false");
    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.FREE),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void listStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("loadDirectChildren", "false");
    params.put("loadMetadataType", "Always");

    String result = new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.LIST_STATUS), params, HttpMethod.GET,
        null).call();
    List<FileInfo> fileInfos =
        new ObjectMapper().readValue(result, new TypeReference<List<FileInfo>>() {});
    FileInfo fileInfo = Iterables.getOnlyElement(fileInfos);
    Assert.assertEquals(uri.getPath(), fileInfo.getPath());
    Assert.assertEquals(0, fileInfo.getLength());
  }

  @Test
  public void mount() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("ufsPath", mFolder.newFolder().getAbsolutePath());

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.MOUNT),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void remove() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("recursive", "false");
    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.REMOVE),
        params, "POST", null).run();

    try {
      mFileSystemMaster.getFileInfo(uri, GET_STATUS_OPTIONS);
      Assert.fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
  }

  @Test
  public void rename() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("/file1");
    AlluxioURI uri2 = new AlluxioURI("/file2");
    mFileSystemMaster.createFile(uri1, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri1, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("srcPath", uri1.toString());
    params.put("dstPath", uri2.toString());
    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.RENAME),
        params, HttpMethod.POST, null).run();

    try {
      mFileSystemMaster.getFileInfo(uri1, GET_STATUS_OPTIONS);
      Assert.fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
    mFileSystemMaster.getFileInfo(uri2, GET_STATUS_OPTIONS);
  }

  @Test
  public void scheduleAsyncPersist() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SCHEDULE_ASYNC_PERSIST), params,
        HttpMethod.POST, null).run();
  }

  @Test
  public void setAttribute() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("pinned", "true");
    params.put("ttl", "100000");
    params.put("ttlAction", TtlAction.DELETE.toString());
    params.put("persisted", "true");
    params.put("recursive", "false");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SET_ATTRIBUTE), params,
        HttpMethod.POST, null).run();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri, GET_STATUS_OPTIONS);
    Assert.assertEquals(uri.toString(), fileInfo.getPath());
    Assert.assertTrue(fileInfo.isPinned());
    Assert.assertEquals(100000, fileInfo.getTtl());
    Assert.assertEquals(TtlAction.DELETE, fileInfo.getTtlAction());
    Assert.assertTrue(fileInfo.isPersisted());
  }

  @Test
  public void unmount() throws Exception {
    AlluxioURI uri = new AlluxioURI("/mount");
    mFileSystemMaster.mount(uri, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        MountOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.UNMOUNT),
        params, HttpMethod.POST, null).run();
  }
}
