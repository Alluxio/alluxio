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

package alluxio.underfs.tos;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.ListObjectsType2Input;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@link TOSUnderFileSystem}.
 */
public class TOSUnderFileSystemTest {
  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static final InstancedConfiguration CONF = Configuration.copyGlobal();

  private static final String BUCKET_NAME = "bucket";

  private TOSUnderFileSystem mTOSUnderFileSystem;
  private TOSV2 mClient;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void setUp() {
    mClient = Mockito.mock(TOSV2.class);
    mTOSUnderFileSystem = new TOSUnderFileSystem(new AlluxioURI("tos://" + BUCKET_NAME),
        mClient, BUCKET_NAME, UnderFileSystemConfiguration.defaults(CONF));
  }

  /**
   * Test case for {@link TOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnTosClientException() throws IOException {
    Mockito.when(mClient.listObjectsType2(ArgumentMatchers.any(ListObjectsType2Input.class)))
        .thenThrow(TosClientException.class);

    boolean ret =
        mTOSUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(ret);
  }

  /**
   * Test case for {@link TOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnTosClientException() throws IOException {
    Mockito.when(mClient.listObjectsType2(ArgumentMatchers.any(ListObjectsType2Input.class)))
        .thenThrow(TosClientException.class);

    boolean ret =
        mTOSUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(ret);
  }

  @Test
  public void isFile404() throws IOException {
    TosServerException e = new TosServerException(404);
    Mockito.when(
        mClient.headObject(ArgumentMatchers.any(HeadObjectV2Input.class)))
        .thenThrow(e);

    Assert.assertFalse(mTOSUnderFileSystem.isFile(SRC));
  }

  @Test
  public void isFileException() throws IOException {
    TosServerException e = new TosServerException(403);
    Mockito.when(
        mClient.headObject(ArgumentMatchers.any(HeadObjectV2Input.class)))
        .thenThrow(e);

    mThrown.expect(AlluxioTosException.class);
    mTOSUnderFileSystem.isFile(SRC);
  }

  /**
   * Test case for {@link TOSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnTosClientException() throws IOException {
    Mockito.when(
        mClient.headObject(ArgumentMatchers.any(HeadObjectV2Input.class)))
        .thenThrow(TosClientException.class);

    mThrown.expect(AlluxioTosException.class);
    mTOSUnderFileSystem.renameFile(SRC, DST);
  }

  @Test
  public void createCredentialsFromConf() {
    // Assume appropriate methods to set configuration and create credentials provider
    Map<PropertyKey, Object> conf = new HashMap<>();
    conf.put(PropertyKey.TOS_ACCESS_KEY, "key1");
    conf.put(PropertyKey.TOS_SECRET_KEY, "key2");

    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(CONF);
  }

  @Test
  public void stripPrefixIfPresent() {
    Assert.assertEquals("", mTOSUnderFileSystem.stripPrefixIfPresent("tos://" + BUCKET_NAME));
    Assert.assertEquals("", mTOSUnderFileSystem.stripPrefixIfPresent("tos://" + BUCKET_NAME + "/"));
    Assert.assertEquals("test/",
        mTOSUnderFileSystem.stripPrefixIfPresent("tos://" + BUCKET_NAME + "/test/"));
    Assert.assertEquals("test", mTOSUnderFileSystem.stripPrefixIfPresent("test"));
    Assert.assertEquals("test/", mTOSUnderFileSystem.stripPrefixIfPresent("test/"));
    Assert.assertEquals("test/", mTOSUnderFileSystem.stripPrefixIfPresent("/test/"));
    Assert.assertEquals("test", mTOSUnderFileSystem.stripPrefixIfPresent("/test"));
    Assert.assertEquals("", mTOSUnderFileSystem.stripPrefixIfPresent(""));
    Assert.assertEquals("", mTOSUnderFileSystem.stripPrefixIfPresent("/"));
  }

  @Test
  public void getFolderSuffix() {
    Assert.assertEquals("/", mTOSUnderFileSystem.getFolderSuffix());
  }
}
