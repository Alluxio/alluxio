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

package alluxio.client.rest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.proxy.PathsRestServiceHandler;
import alluxio.proxy.StreamsRestServiceHandler;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link StreamsRestServiceHandler}.
 */
public final class FileSystemClientRestApiTest extends RestApiTest {
  private static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();

  private static final Map<String, String> NO_PARAMS = new HashMap<>();
  private static final String PATHS_PREFIX = "paths/";
  private static final String STREAMS_PREFIX = "streams/";
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getProxyProcess().getWebLocalPort();
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  @Test
  public void createDirectory() throws Exception {
    AlluxioURI uri = new AlluxioURI("/dir");
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.CREATE_DIRECTORY, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getCreateDirectoryOptions()))
            .run();
    assertTrue(
        mFileSystemMaster
            .listStatus(uri,
                ListStatusContext
                    .defaults(FileSystemClientOptions.getListStatusOptions().toBuilder()))
        .isEmpty());
  }

  @Test
  public void delete() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.DELETE, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getDeleteOptions())).run();
    try {
      mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT);
      fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
  }

  @Test
  public void download() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    String message = "Greetings traveller!";
    writeFile(uri, message.getBytes());
    assertEquals(message, new String(readFile(uri)));
  }

  @Test
  public void exists() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.EXISTS, NO_PARAMS,
        HttpMethod.POST, true,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getExistsOptions())).run();
  }

  @Test
  public void free() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.FREE, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getFreeOptions())).run();
  }

  @Test
  public void getStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.GET_STATUS, NO_PARAMS,
        HttpMethod.POST,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getGetStatusOptions())).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    assertEquals(uri.getPath(), fileInfo.getPath());
    assertEquals(0, fileInfo.getLength());
  }

  @Test
  public void listStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.LIST_STATUS, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getListStatusOptions())).call();
    List<FileInfo> fileInfos =
        new ObjectMapper().readValue(result, new TypeReference<List<FileInfo>>() {});
    FileInfo fileInfo = Iterables.getOnlyElement(fileInfos);
    assertEquals(uri.getPath(), fileInfo.getPath());
    assertEquals(0, fileInfo.getLength());
  }

  @Test
  public void mount() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    Map<String, String> params = new HashMap<>();
    params.put("src", mFolder.newFolder().getAbsolutePath());
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.MOUNT, params,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getMountOptions())).run();
  }

  @Test
  public void rename() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("/file1");
    AlluxioURI uri2 = new AlluxioURI("/file2");
    writeFile(uri1, null);
    Map<String, String> params = new HashMap<>();
    params.put("dst", uri2.toString());
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri1.toString() + "/" + PathsRestServiceHandler.RENAME, params,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getRenameOptions())).run();
    try {
      mFileSystemMaster.getFileInfo(uri1, GET_STATUS_CONTEXT);
      fail("file should have been removed");
    } catch (FileDoesNotExistException e) {
      // Expected
    }
    mFileSystemMaster.getFileInfo(uri2, GET_STATUS_CONTEXT);
  }

  @Test
  public void setAttribute() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.SET_ATTRIBUTE, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(FileSystemClientOptions
            .getSetAttributeOptions().toBuilder().setMode(Mode.defaults().toShort()).build()))
                .run();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT);
    assertEquals(uri.toString(), fileInfo.getPath());
  }

  @Test
  public void unmount() throws Exception {
    AlluxioURI uri = new AlluxioURI("/mount");
    mFileSystemMaster.mount(uri, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        MountContext.defaults());
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.UNMOUNT, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getUnmountOptions())).run();
  }

  @Test
  public void upload() throws Exception  {
    AlluxioURI uri = new AlluxioURI("/file");
    String message = "Greetings traveller!";
    writeFile(uri, message.getBytes());
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.GET_STATUS, NO_PARAMS,
        HttpMethod.POST, null).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    assertEquals(message.length(), fileInfo.getLength());
  }

  private byte[] readFile(AlluxioURI path) throws Exception {
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + path.toString() + "/" + PathsRestServiceHandler.OPEN_FILE, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getOpenFileOptions())).call();
    Integer id = new ObjectMapper().readValue(result, Integer.TYPE);
    result = new TestCase(mHostname, mPort,
        STREAMS_PREFIX + id.toString() + "/" + StreamsRestServiceHandler.READ, NO_PARAMS,
        HttpMethod.POST, null).call();
    new TestCase(mHostname, mPort,
        STREAMS_PREFIX + id.toString() + "/" + StreamsRestServiceHandler.CLOSE, NO_PARAMS,
        HttpMethod.POST, null).run();
    return result.getBytes();
  }

  private void writeFile(AlluxioURI path, byte[] input) throws Exception {
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + path.toString() + "/" + PathsRestServiceHandler.CREATE_FILE, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(FileSystemClientOptions.getCreateFileOptions())).call();
    Integer id = new ObjectMapper().readValue(result, Integer.TYPE);
    TestCaseOptions options = TestCaseOptions.defaults();
    long expected = 0;
    if (input != null) {
      options.setInputStream(new ByteArrayInputStream(input));
      expected = input.length;
    }
    new TestCase(mHostname, mPort,
        STREAMS_PREFIX + id.toString() + "/" + StreamsRestServiceHandler.WRITE, NO_PARAMS,
        HttpMethod.POST, expected, options).run();
    new TestCase(mHostname, mPort,
        STREAMS_PREFIX + id.toString() + "/" + StreamsRestServiceHandler.CLOSE, NO_PARAMS,
        HttpMethod.POST, null).run();
  }
}
