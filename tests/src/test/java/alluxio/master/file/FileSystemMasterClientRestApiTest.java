/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.AlluxioMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.rest.TestCaseFactory;
import alluxio.util.CommonUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileBlockInfoTest;
import alluxio.wire.FileInfo;
import alluxio.wire.FileInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
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

/**
 * Test cases for {@link FileSystemMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
@Ignore("https://alluxio.atlassian.net/browse/ALLUXIO-1888")
public class FileSystemMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private FileSystemMaster mFileSystemMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @Before
  public void before() throws Exception {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mFileSystemMaster = PowerMockito.mock(FileSystemMaster.class);
    // Replace the file system master created by LocalAlluxioClusterResource with a mock.
    FileSystemMaster fileSystemMaster =
        Whitebox.getInternalState(alluxioMaster, "mFileSystemMaster");
    fileSystemMaster.stop();
    Whitebox.setInternalState(alluxioMaster, "mFileSystemMaster", mFileSystemMaster);
  }

  private String getEndpoint(String suffix) {
    return FileSystemMasterClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_NAME),
            NO_PARAMS, "GET", Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.SERVICE_VERSION),
            NO_PARAMS, "GET", Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void completeFileTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("ufsLength", "1");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.COMPLETE_FILE),
            params, "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster)
        .completeFile(Mockito.<AlluxioURI>any(), Mockito.<CompleteFileOptions>any());
  }

  @Test
  public void createDirectoryTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("allowExists", "false");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_DIRECTORY),
            params, "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster)
        .createDirectory(Mockito.<AlluxioURI>any(), Mockito.<CreateDirectoryOptions>any());
  }

  @Test
  public void createFileTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("persisted", "false");
    params.put("recursive", "false");
    params.put("blockSizeBytes", "1");
    params.put("blockSizeBytes", "1");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.CREATE_FILE),
            params, "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster)
        .createFile(Mockito.<AlluxioURI>any(), Mockito.<CreateFileOptions>any());
  }

  @Test
  public void getFileBlockInfoListTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    Random random = new Random();
    List<FileBlockInfo> fileBlockInfos = Lists.newArrayList();
    int numFileBlockInfos = random.nextInt(10);
    for (int i = 0; i < numFileBlockInfos; i++) {
      fileBlockInfos.add(FileBlockInfoTest.createRandom());
    }
    Mockito.doReturn(fileBlockInfos).when(mFileSystemMaster)
        .getFileBlockInfoList(Mockito.<AlluxioURI>any());

    TestCaseFactory.newMasterTestCase(
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_FILE_BLOCK_INFO_LIST), params,
        "GET", fileBlockInfos, mResource).run();

    Mockito.verify(mFileSystemMaster).getFileBlockInfoList(Mockito.<AlluxioURI>any());
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    Random random = new Random();
    long newBlockId = random.nextLong();
    Mockito.doReturn(newBlockId).when(mFileSystemMaster)
        .getNewBlockIdForFile(Mockito.<AlluxioURI>any());

    TestCaseFactory.newMasterTestCase(
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_NEW_BLOCK_ID_FOR_FILE), params,
        "POST", newBlockId, mResource).run();

    Mockito.verify(mFileSystemMaster).getNewBlockIdForFile(Mockito.<AlluxioURI>any());
  }

  @Test
  public void getStatusTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    FileInfo fileInfo = FileInfoTest.createRandom();
    Mockito.doReturn(fileInfo).when(mFileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.GET_STATUS), params,
            "GET", fileInfo, mResource).run();

    Mockito.verify(mFileSystemMaster).getFileInfo(Mockito.<AlluxioURI>any());
  }

  @Test
  public void getStatusInternalTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("fileId", "1");

    FileInfo fileInfo = FileInfoTest.createRandom();
    Mockito.doReturn(fileInfo).when(mFileSystemMaster).getFileInfo(Mockito.anyLong());

    TestCaseFactory.newMasterTestCase(
        getEndpoint(FileSystemMasterClientRestServiceHandler.GET_STATUS_INTERNAL), params, "GET",
        fileInfo, mResource).run();

    Mockito.verify(mFileSystemMaster).getFileInfo(Mockito.anyLong());
  }

  @Test
  public void getUfsAddress() throws Exception {
    String ufsAddress = CommonUtils.randomString(10);
    Mockito.doReturn(ufsAddress).when(mFileSystemMaster).getUfsAddress();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.GET_UFS_ADDRESS),
            NO_PARAMS, "GET", ufsAddress, mResource).run();

    Mockito.verify(mFileSystemMaster).getUfsAddress();
  }

  @Test
  public void freeTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("recursive", "false");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.FREE), params,
            "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster).free(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void listStatusTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    Random random = new Random();
    List<FileInfo> fileInfos = Lists.newArrayList();
    int numFileInfos = random.nextInt(10);
    for (int i = 0; i < numFileInfos; i++) {
      fileInfos.add(FileInfoTest.createRandom());
    }
    Mockito.doReturn(fileInfos).when(mFileSystemMaster).getFileInfoList(Mockito.<AlluxioURI>any());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.LIST_STATUS),
            params, "GET", fileInfos, mResource).run();

    Mockito.verify(mFileSystemMaster).getFileInfoList(Mockito.<AlluxioURI>any());
  }

  @Test
  public void loadMetadataTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("recursive", "false");

    Random random = new Random();
    long loadMetadataResult = random.nextLong();
    Mockito.doReturn(loadMetadataResult).when(mFileSystemMaster)
        .loadMetadata(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.LOAD_METADATA),
            params, "POST", loadMetadataResult, mResource).run();

    Mockito.verify(mFileSystemMaster).loadMetadata(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void mountTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("ufsPath", "test");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.MOUNT), params,
            "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster)
        .mount(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any(), Mockito.<MountOptions>any());
  }

  @Test
  public void removeTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("recursive", "false");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.REMOVE), params,
            "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster).delete(Mockito.<AlluxioURI>any(), Mockito.anyBoolean());
  }

  @Test
  public void renameTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("srcPath", "test");
    params.put("dstPath", "test");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.RENAME), params,
            "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster).rename(Mockito.<AlluxioURI>any(), Mockito.<AlluxioURI>any());
  }

  @Test
  public void scheduleAsyncPersistTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    TestCaseFactory.newMasterTestCase(
        getEndpoint(FileSystemMasterClientRestServiceHandler.SCHEDULE_ASYNC_PERSIST), params,
        "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster).scheduleAsyncPersistence(Mockito.<AlluxioURI>any());
  }

  @Test
  public void setAttributeTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("pinned", "false");
    params.put("ttl", "0");
    params.put("persisted", "false");
    params.put("owner", "test");
    params.put("group", "test");
    params.put("permission", "644");
    params.put("recursive", "false");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.SET_ATTRIBUTE),
            params, "POST", null, mResource).run();

    Mockito.verify(mFileSystemMaster)
        .setAttribute(Mockito.<AlluxioURI>any(), Mockito.<SetAttributeOptions>any());
  }

  @Test
  public void unmountTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    Random random = new Random();
    boolean unmountResult = random.nextBoolean();
    Mockito.doReturn(unmountResult).when(mFileSystemMaster).unmount(Mockito.<AlluxioURI>any());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(FileSystemMasterClientRestServiceHandler.UNMOUNT), params,
            "POST", unmountResult, mResource).run();

    Mockito.verify(mFileSystemMaster).unmount(Mockito.<AlluxioURI>any());
  }
}
