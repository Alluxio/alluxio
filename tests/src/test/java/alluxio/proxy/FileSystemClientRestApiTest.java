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
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Assert;
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
  private static final Map<String, String> NO_PARAMS = new HashMap<>();
  private static final String PATHS_PREFIX = "paths/";
  private static final String STREAMS_PREFIX = "streams/";
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getProxy().getWebLocalPort();
    mFileSystemMaster =
        mResource.get().getMaster().getInternalMaster().getMaster(FileSystemMaster.class);
  }

  @Test
  public void createDirectory() throws Exception {
    AlluxioURI uri = new AlluxioURI("/dir");
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.CREATE_DIRECTORY, NO_PARAMS,
        HttpMethod.POST, null,
        TestCaseOptions.defaults().setBody(CreateDirectoryOptions.defaults())).run();
    Assert.assertTrue(
        mFileSystemMaster.listStatus(uri, alluxio.master.file.options.ListStatusOptions.defaults())
            .isEmpty());
  }

  @Test
  public void delete() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.DELETE, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(DeleteOptions.defaults())).run();
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
    writeFile(uri, message.getBytes());
    Assert.assertEquals(message, new String(readFile(uri)));
  }

  @Test
  public void exists() throws Exception  {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.EXISTS, NO_PARAMS,
        HttpMethod.POST, true, TestCaseOptions.defaults().setBody(ExistsOptions.defaults())).run();
  }

  @Test
  public void free() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.FREE, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(FreeOptions.defaults())).run();
  }

  @Test
  public void getStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.GET_STATUS, NO_PARAMS,
        HttpMethod.POST, TestCaseOptions.defaults().setBody(GetStatusOptions.defaults())).call();
    FileInfo fileInfo = new ObjectMapper().readValue(result, FileInfo.class);
    Assert.assertEquals(uri.getPath(), fileInfo.getPath());
    Assert.assertEquals(0, fileInfo.getLength());
  }

  @Test
  public void listStatus() throws Exception {
    AlluxioURI uri = new AlluxioURI("/file");
    writeFile(uri, null);
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.LIST_STATUS, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(ListStatusOptions.defaults()))
        .call();
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
    params.put("src", mFolder.newFolder().getAbsolutePath());
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.MOUNT, params,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(MountOptions.defaults())).run();
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
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(RenameOptions.defaults())).run();
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
    writeFile(uri, null);
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.SET_ATTRIBUTE, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults()
        .setBody(SetAttributeOptions.defaults().setMode(Mode.defaults())))
        .run();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri);
    Assert.assertEquals(uri.toString(), fileInfo.getPath());
  }

  @Test
  public void unmount() throws Exception {
    AlluxioURI uri = new AlluxioURI("/mount");
    mFileSystemMaster.mount(uri, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        alluxio.master.file.options.MountOptions.defaults());
    new TestCase(mHostname, mPort,
        PATHS_PREFIX + uri.toString() + "/" + PathsRestServiceHandler.UNMOUNT, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(UnmountOptions.defaults())).run();
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
    Assert.assertEquals(message.length(), fileInfo.getLength());
  }

  private byte[] readFile(AlluxioURI path) throws Exception {
    String result = new TestCase(mHostname, mPort,
        PATHS_PREFIX + path.toString() + "/" + PathsRestServiceHandler.OPEN_FILE, NO_PARAMS,
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(OpenFileOptions.defaults()))
        .call();
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
        HttpMethod.POST, null, TestCaseOptions.defaults().setBody(CreateFileOptions.defaults()))
        .call();
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
