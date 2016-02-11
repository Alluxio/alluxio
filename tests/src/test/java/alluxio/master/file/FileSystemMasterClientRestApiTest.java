/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.file;

import alluxio.AbstractRestApiTest;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.master.AlluxioMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileBlockInfoTest;
import alluxio.wire.FileInfo;
import alluxio.wire.FileInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public class FileSystemMasterClientRestApiTest extends AbstractRestApiTest {

  @Override
  @Test
  public void endpointsTest() throws Exception {
    // Create test input values.
    Map<String, String> completeFileParams = Maps.newHashMap();
    completeFileParams.put("path", "test");
    completeFileParams.put("ufsLength", "1");
    Map<String, String> createDirectoryParams = Maps.newHashMap();
    createDirectoryParams.put("path", "test");
    createDirectoryParams.put("persisted", "false");
    createDirectoryParams.put("recursive", "false");
    createDirectoryParams.put("allowExists", "false");
    Map<String, String> getFileBlockInfoListParams = Maps.newHashMap();
    getFileBlockInfoListParams.put("path", "test");
    Map<String, String> getNewBlockIdForFileParams = Maps.newHashMap();
    getNewBlockIdForFileParams.put("path", "test");
    Map<String, String> getStatusParams = Maps.newHashMap();
    getStatusParams.put("path", "test");
    Map<String, String> getStatusInternalParams = Maps.newHashMap();
    getStatusInternalParams.put("fileId", "1");
    Map<String, String> freeParams = Maps.newHashMap();
    freeParams.put("path", "test");
    freeParams.put("recursive", "false");
    Map<String, String> listStatusParams = Maps.newHashMap();
    listStatusParams.put("path", "test");
    Map<String, String> loadMetadataParams = Maps.newHashMap();
    loadMetadataParams.put("path", "test");
    loadMetadataParams.put("recursive", "false");
    Map<String, String> mountParams = Maps.newHashMap();
    mountParams.put("path", "test");
    mountParams.put("ufsPath", "test");
    Map<String, String> removeParams = Maps.newHashMap();
    removeParams.put("path", "test");
    removeParams.put("recursive", "false");
    Map<String, String> renameParams = Maps.newHashMap();
    renameParams.put("srcPath", "test");
    renameParams.put("dstPath", "test");
    Map<String, String> scheduleAsyncPersistParams = Maps.newHashMap();
    scheduleAsyncPersistParams.put("path", "test");
    Map<String, String> setAttributeParams = Maps.newHashMap();
    setAttributeParams.put("path", "test");
    setAttributeParams.put("pinned", "false");
    setAttributeParams.put("ttl", "0");
    setAttributeParams.put("persisted", "false");
    setAttributeParams.put("owner", "test");
    setAttributeParams.put("group", "test");
    setAttributeParams.put("permission", "644");
    setAttributeParams.put("recursive", "false");
    Map<String, String> unmountParams = Maps.newHashMap();
    unmountParams.put("path", "test");

    // Generate random return values.
    Random random = new Random();
    List<FileBlockInfo> fileBlockInfos = Lists.newArrayList();
    int numFileBlockInfos = random.nextInt(10);
    for (int i = 0; i < numFileBlockInfos; i++) {
      fileBlockInfos.add(FileBlockInfoTest.createRandom());
    }
    List<FileInfo> fileInfos = Lists.newArrayList();
    int numFileInfos = random.nextInt(10);
    for (int i = 0; i < numFileInfos; i++) {
      fileInfos.add(FileInfoTest.createRandom());
    }
    long newBlockId = random.nextLong();
    FileInfo fileInfo = FileInfoTest.createRandom();
    long loadMetadataResult = random.nextLong();
    long scheduleAsyncPersistResult = random.nextLong();
    String ufsAddress = "";
    int ufsAddressLength = random.nextInt(10);
    for (int i = 0; i < ufsAddressLength; i++) {
      ufsAddress += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }
    boolean unmountResult = random.nextBoolean();

    // Set up mocks.
    FileSystemMaster fileSystemMaster = PowerMockito.mock(FileSystemMaster.class);
    Mockito.doReturn(fileBlockInfos).when(fileSystemMaster)
        .getFileBlockInfoList(Mockito.<AlluxioURI>any());
    Mockito.doReturn(fileInfo).when(fileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());
    Mockito.doReturn(fileInfo).when(fileSystemMaster).getFileInfo(Mockito.anyLong());
    Mockito.doReturn(fileInfos).when(fileSystemMaster).getFileInfoList(Mockito.<AlluxioURI>any());
    Mockito.doReturn(newBlockId).when(fileSystemMaster)
        .getNewBlockIdForFile(Mockito.<AlluxioURI>any());
    Mockito.doReturn(ufsAddress).when(fileSystemMaster).getUfsAddress();
    Mockito.doReturn(loadMetadataResult).when(fileSystemMaster)
        .loadMetadata(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
    Mockito.doReturn(scheduleAsyncPersistResult).when(fileSystemMaster)
        .scheduleAsyncPersistence(Mockito.<AlluxioURI>any());
    Mockito.doReturn(unmountResult).when(fileSystemMaster).unmount(Mockito.<AlluxioURI>any());
    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    Mockito.when(alluxioMaster.getFileSystemMaster()).thenReturn(fileSystemMaster);
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);

    // Create test cases.
    List<TestCase> testCases = Lists.newArrayList();
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.SERVICE_NAME,
        Maps.<String, String>newHashMap(), "GET",
        Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.SERVICE_VERSION,
        Maps.<String, String>newHashMap(), "GET",
        Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.COMPLETE_FILE,
        completeFileParams, "POST", ""));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.CREATE_DIRECTORY,
        createDirectoryParams, "POST", ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.GET_FILE_BLOCK_INFO_LIST,
            getFileBlockInfoListParams, "GET", fileBlockInfos));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.GET_NEW_BLOCK_ID_FOR_FILE,
            getNewBlockIdForFileParams, "POST", newBlockId));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.GET_STATUS, getStatusParams,
            "GET", fileInfo));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.GET_STATUS_INTERNAL,
        getStatusInternalParams, "GET", fileInfo));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.GET_UFS_ADDRESS,
        Maps.<String, String>newHashMap(), "GET", ufsAddress));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.FREE, freeParams, "POST", ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.LIST_STATUS, listStatusParams,
            "GET", fileInfos));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.LOAD_METADATA,
        loadMetadataParams, "POST", loadMetadataResult));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.MOUNT, mountParams, "POST",
            ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.REMOVE, removeParams, "POST",
            ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.RENAME, renameParams, "POST",
            ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.SCHEDULE_ASYNC_PERSIST,
            scheduleAsyncPersistParams, "POST", scheduleAsyncPersistResult));
    testCases.add(new MasterTestCase(FileSystemMasterClientRestServiceHandler.SET_ATTRIBUTE,
        setAttributeParams, "PUT", ""));
    testCases.add(
        new MasterTestCase(FileSystemMasterClientRestServiceHandler.UNMOUNT, unmountParams, "POST",
            unmountResult));

    // Execute test cases.
    run(testCases);

    // Verify invocations.
    Mockito.verify(fileSystemMaster)
        .completeFile(Mockito.<AlluxioURI>any(), Mockito.<CompleteFileOptions>any());
    Mockito.verify(fileSystemMaster).deleteFile(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
    Mockito.verify(fileSystemMaster).getFileBlockInfoList(Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).getFileInfo(Mockito.anyLong());
    Mockito.verify(fileSystemMaster).getFileInfoList(Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).getNewBlockIdForFile(Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).getUfsAddress();
    Mockito.verify(fileSystemMaster).free(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
    Mockito.verify(fileSystemMaster).loadMetadata(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
    Mockito.verify(fileSystemMaster)
        .mkdir(Mockito.<AlluxioURI>any(), Mockito.<CreateDirectoryOptions>any());
    Mockito.verify(fileSystemMaster).mount(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).rename(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster).scheduleAsyncPersistence(Mockito.<AlluxioURI>any());
    Mockito.verify(fileSystemMaster)
        .setAttribute(Mockito.<AlluxioURI>any(), Mockito.<SetAttributeOptions>any());
    Mockito.verify(fileSystemMaster).unmount(Mockito.<AlluxioURI>any());
  }
}
