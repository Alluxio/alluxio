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
import alluxio.master.AlluxioMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.FileInfo;
import alluxio.wire.FileInfoTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link FileSystemMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class FileSystemMasterClientRestApiTest extends RestApiTest {
  private FileSystemMaster mFileSystemMaster;

  @Before
  public void before() throws Exception {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mFileSystemMaster = PowerMockito.mock(FileSystemMaster.class);
    // Replace the file system master created by LocalAlluxioClusterResource with a mock.
    FileSystemMaster fileSystemMaster =
        Whitebox.getInternalState(alluxioMaster, "mFileSystemMaster");
    fileSystemMaster.stop();
    Whitebox.setInternalState(alluxioMaster, "mFileSystemMaster", mFileSystemMaster);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getWebLocalPort();
    mServicePrefix = FileSystemMasterClientRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void serviceNameTest() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_NAME), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_VERSION), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void completeFileTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("ufsLength", "1");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.COMPLETE_FILE), params,
        HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster)
        .completeFile(Mockito.<AlluxioURI>any(), Mockito.<CompleteFileOptions>any());
  }

  @Test
  public void createDirectoryTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("allowExists", "false");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_DIRECTORY), params,
        HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster)
        .createDirectory(Mockito.<AlluxioURI>any(), Mockito.<CreateDirectoryOptions>any());
  }

  @Test
  public void createFileTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("blockSizeBytes", "1");
    params.put("blockSizeBytes", "1");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_FILE), params, HttpMethod.POST,
        null).run();

    Mockito.verify(mFileSystemMaster)
        .createFile(Mockito.<AlluxioURI>any(), Mockito.<CreateFileOptions>any());
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    Random random = new Random();
    long newBlockId = random.nextLong();
    Mockito.doReturn(newBlockId).when(mFileSystemMaster)
        .getNewBlockIdForFile(Mockito.<AlluxioURI>any());

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_NEW_BLOCK_ID_FOR_FILE), params,
        HttpMethod.POST, newBlockId).run();

    Mockito.verify(mFileSystemMaster).getNewBlockIdForFile(Mockito.<AlluxioURI>any());
  }

  @Test
  public void getStatusTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    FileInfo fileInfo = FileInfoTest.createRandom();
    Mockito.doReturn(fileInfo).when(mFileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.GET_STATUS),
        params, HttpMethod.GET, fileInfo).run();

    Mockito.verify(mFileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());
  }

  @Test
  public void freeTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("recursive", "false");

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.FREE),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster).free(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void listStatusTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    Random random = new Random();
    List<FileInfo> fileInfos = new ArrayList<>();
    int numFileInfos = random.nextInt(10);
    for (int i = 0; i < numFileInfos; i++) {
      fileInfos.add(FileInfoTest.createRandom());
    }
    Mockito.doReturn(fileInfos).when(mFileSystemMaster)
        .getFileInfoList(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.LIST_STATUS), params, HttpMethod.GET,
        fileInfos).run();

    Mockito.verify(mFileSystemMaster)
        .getFileInfoList(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void mountTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("ufsPath", "test");

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.MOUNT),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster)
        .mount(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any(), Mockito.<MountOptions>any());
  }

  @Test
  public void removeTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("recursive", "false");

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.REMOVE),
        params, "POST", null).run();

    Mockito.verify(mFileSystemMaster).delete(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void renameTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("srcPath", "test");
    params.put("dstPath", "test");

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.RENAME),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster).rename(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any());
  }

  @Test
  public void scheduleAsyncPersistTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SCHEDULE_ASYNC_PERSIST), params,
        HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster).scheduleAsyncPersistence(Mockito.<AlluxioURI>any());
  }

  @Test
  public void setAttributeTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("pinned", "false");
    params.put("ttl", "0");
    params.put("persisted", "false");
    params.put("owner", "test");
    params.put("group", "test");
    params.put("permission", "644");
    params.put("recursive", "false");

    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemMasterClientRestServiceHandler.SET_ATTRIBUTE), params,
        HttpMethod.POST, null).run();

    Mockito.verify(mFileSystemMaster)
        .setAttribute(Mockito.<AlluxioURI>any(), Mockito.<SetAttributeOptions>any());
  }

  @Test
  public void unmountTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    Random random = new Random();
    boolean unmountResult = random.nextBoolean();
    Mockito.doReturn(unmountResult).when(mFileSystemMaster).unmount(Mockito.<AlluxioURI>any());

    new TestCase(mHostname, mPort, getEndpoint(FileSystemMasterClientRestServiceHandler.UNMOUNT),
        params, HttpMethod.POST, unmountResult).run();

    Mockito.verify(mFileSystemMaster).unmount(Mockito.<AlluxioURI>any());
  }
}
