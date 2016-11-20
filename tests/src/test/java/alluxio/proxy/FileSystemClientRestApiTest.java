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

package alluxio.proxy;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link FileSystemClientRestServiceHandler}.
 */
public final class FileSystemClientRestApiTest extends RestApiTest {
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getProxy().getWebLocalPort();
    mServicePrefix = FileSystemClientRestServiceHandler.SERVICE_PREFIX;
    mFileSystemMaster = mResource.get().getMaster().getInternalMaster().getFileSystemMaster();
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemClientRestServiceHandler.SERVICE_NAME), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemClientRestServiceHandler.SERVICE_VERSION), NO_PARAMS,
        HttpMethod.GET, Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void createDirectory() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("allowExists", "false");
    params.put("persisted", "false");
    params.put("recursive", "false");
    new TestCase(mHostname, mPort,
        getEndpoint(FileSystemClientRestServiceHandler.CREATE_DIRECTORY), params,
        HttpMethod.POST, null).run();

    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void delete() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("recursive", "false");
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.DELETE),
        params, "POST", null).run();

    try {
      mFileSystemMaster.getFileInfo(uri);
      Assert.fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
  }

  @Test
  public void download() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    String message = "Greetings traveller!";
    uploadFile(uri, message.getBytes());
    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    String result =
        new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.DOWNLOAD),
            params, HttpMethod.GET, null).call();
    Assert.assertEquals(message, result);
  }

  @Test
  public void exists() throws Exception  {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.EXISTS),
        params, HttpMethod.GET, true).run();
  }

  @Test
  public void free() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    params.put("recursive", "false");
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.FREE),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void getStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    mFileSystemMaster.createFile(uri, CreateFileOptions.defaults());
    mFileSystemMaster.completeFile(uri, CompleteFileOptions.defaults());

    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    String result = new TestCase(mHostname, mPort,
        getEndpoint(FileSystemClientRestServiceHandler.GET_STATUS), params, HttpMethod.GET,
        null).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    Assert.assertEquals(uri.getPath(), fileInfo.getPath());
    Assert.assertEquals(0, fileInfo.getLength());
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
        getEndpoint(FileSystemClientRestServiceHandler.LIST_STATUS), params, HttpMethod.GET,
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

    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.MOUNT),
        params, HttpMethod.POST, null).run();
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
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.RENAME),
        params, HttpMethod.POST, null).run();

    try {
      mFileSystemMaster.getFileInfo(uri1);
      Assert.fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
    mFileSystemMaster.getFileInfo(uri2);
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
        getEndpoint(FileSystemClientRestServiceHandler.SET_ATTRIBUTE), params,
        HttpMethod.POST, null).run();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri);
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
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.UNMOUNT),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void upload() throws Exception  {
    AlluxioURI uri = new AlluxioURI("/file");
    String message = "Greetings traveller!";
    uploadFile(uri, message.getBytes());
    Map<String, String> params = new HashMap<>();
    params.put("path", uri.toString());
    String result =
        new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.GET_STATUS),
            params, HttpMethod.GET, null).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    Assert.assertEquals(message.length(), fileInfo.getLength());
  }

  private void uploadFile(AlluxioURI path, byte[] input) throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", path.toString());
    InputStream inputStream = new ByteArrayInputStream(input);
    new TestCase(mHostname, mPort, getEndpoint(FileSystemClientRestServiceHandler.UPLOAD), params,
        HttpMethod.POST, null, TestCaseOptions.defaults().setInputStream(inputStream)).run();
  }
}
