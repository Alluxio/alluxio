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

package alluxio.server.worker;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.http.ResponseFileInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;

public class WorkerHttpServerIntegrationTest {

  private final int mHttpServerPort = 28080;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_HTTP_SERVER_PORT, mHttpServerPort)
          .setProperty(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED, false)
          .build();

  private static final String TEST_CONTENT = "test-content";

  private FileSystem mFileSystem = null;
  private CreateFilePOptions mWriteBoth;

  @Before
  public void before() throws Exception {
    mLocalAlluxioClusterResource.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFilePOptions.newBuilder().setRecursive(true)
        .setWriteType(WritePType.CACHE_THROUGH).build();
  }

  @After
  public void after() throws Exception {
    mFileSystem.close();
    mLocalAlluxioClusterResource.stop();
  }

  @Test
  public void testListStatus() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    String parentPath = uniqPath.substring(0, uniqPath.lastIndexOf("/"));
    // create a directory
    AlluxioURI dirUri = new AlluxioURI(parentPath + "/test-dir");
    mFileSystem.createDirectory(dirUri);
    URIStatus dirStatus = mFileSystem.getStatus(dirUri);
    Assert.assertNotNull(dirStatus);
    // create a file
    AlluxioURI fileUri = new AlluxioURI(parentPath + "/test-file");
    FileOutStream out = mFileSystem.createFile(fileUri, mWriteBoth);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();
    URIStatus fileStatus = mFileSystem.getStatus(fileUri);
    Assert.assertNotNull(fileStatus);
    // try executing list operation by HTTP RESTful API
    CloseableHttpClient httpClient = HttpClients.createDefault();
    URIBuilder uriBuilder = new URIBuilder("http://localhost:"
        + mHttpServerPort + "/v1/files?path=" + parentPath);
    HttpGet httpGet = new HttpGet(uriBuilder.build());
    CloseableHttpResponse response = httpClient.execute(httpGet);
    HttpEntity entity = response.getEntity();
    String entityStr = EntityUtils.toString(entity, "UTF-8");
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<List<ResponseFileInfo>>() {
    }.getType();
    List<ResponseFileInfo> responseFileInfoList = gson.fromJson(entityStr, type);
    Assert.assertEquals(2, responseFileInfoList.size());

    for (ResponseFileInfo responseFileInfo : responseFileInfoList) {
      if (responseFileInfo.getType().equals("directory")) {
        Assert.assertEquals(dirStatus.getName(), responseFileInfo.getName());
        Assert.assertEquals(dirStatus.getPath(), responseFileInfo.getPath());
        Assert.assertEquals(dirStatus.getUfsPath(), responseFileInfo.getUfsPath());
        Assert.assertEquals(dirStatus.getLastModificationTimeMs(),
            responseFileInfo.getLastModificationTimeMs());
        Assert.assertEquals(0, responseFileInfo.getLength());
        Assert.assertEquals("0B", responseFileInfo.getHumanReadableFileSize());
      } else {
        Assert.assertEquals(fileStatus.getName(), responseFileInfo.getName());
        Assert.assertEquals(fileStatus.getPath(), responseFileInfo.getPath());
        Assert.assertEquals(fileStatus.getUfsPath(), responseFileInfo.getUfsPath());
        Assert.assertEquals(fileStatus.getLastModificationTimeMs(),
            responseFileInfo.getLastModificationTimeMs());
        Assert.assertEquals(fileStatus.getLength(), responseFileInfo.getLength());
        Assert.assertEquals(FormatUtils.getSizeFromBytes(fileStatus.getLength()),
            responseFileInfo.getHumanReadableFileSize());
      }
    }
  }

  @Test
  public void testGetStatus() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    String parentPath = uniqPath.substring(0, uniqPath.lastIndexOf("/"));
    // create a directory
    AlluxioURI dirUri = new AlluxioURI(parentPath + "/test-dir");
    mFileSystem.createDirectory(dirUri);
    URIStatus dirStatus = mFileSystem.getStatus(dirUri);
    Assert.assertNotNull(dirStatus);
    // create a file
    AlluxioURI fileUri = new AlluxioURI(parentPath + "/test-file");
    FileOutStream out = mFileSystem.createFile(fileUri, mWriteBoth);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();
    URIStatus fileStatus = mFileSystem.getStatus(fileUri);
    Assert.assertNotNull(fileStatus);
    // try executing getStatus operation by HTTP RESTful API
    CloseableHttpClient httpClient = HttpClients.createDefault();
    URIBuilder uriBuilder = new URIBuilder("http://localhost:"
        + mHttpServerPort + "/v1/info?path=" + parentPath + "/test-dir");
    HttpGet httpGet = new HttpGet(uriBuilder.build());
    CloseableHttpResponse response = httpClient.execute(httpGet);
    HttpEntity entity = response.getEntity();
    String entityStr = EntityUtils.toString(entity, "UTF-8");
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<List<ResponseFileInfo>>() {
    }.getType();
    List<ResponseFileInfo> responseFileInfoList = gson.fromJson(entityStr, type);
    Assert.assertEquals(1, responseFileInfoList.size());

    ResponseFileInfo responseFileInfo = responseFileInfoList.get(0);
    Assert.assertEquals("directory", responseFileInfo.getType());
    Assert.assertEquals(dirStatus.getName(), responseFileInfo.getName());
    Assert.assertEquals(dirStatus.getPath(), responseFileInfo.getPath());
    Assert.assertEquals(dirStatus.getUfsPath(), responseFileInfo.getUfsPath());
    Assert.assertEquals(dirStatus.getLastModificationTimeMs(),
        responseFileInfo.getLastModificationTimeMs());
    Assert.assertEquals(0, responseFileInfo.getLength());
    Assert.assertEquals("0B", responseFileInfo.getHumanReadableFileSize());
  }
}
