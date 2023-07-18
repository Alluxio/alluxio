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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.proxy.s3.ListBucketOptions;
import alluxio.proxy.s3.ListBucketResult;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3RestServiceHandler;
import alluxio.proxy.s3.S3RestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.ModeParser;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

public class ListStatusTest extends RestApiTest {

  private FileSystem mFileSystem;
  private FileSystemMaster mFileSystemMaster;

  @ClassRule
  public static LocalAlluxioClusterResource sResource = new LocalAlluxioClusterResource.Builder()
      .setIncludeProxy(true)
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true) // default
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
          AuthType.SIMPLE) // default, getDefaultOptionsWithAuth() sets the "Authorization" header
      .setProperty(PropertyKey.S3_REST_AUTHENTICATION_ENABLED, // TODO(czhu) refactor this key name
          false) // default, disables AWS "Authorization" header signature validation
      .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "1KB")
      .setProperty(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.MUST_CACHE.name()) // skip UFS
      .setProperty(PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_MIN_PART_SIZE, "0")
      .setProperty(PropertyKey.PROXY_S3_TAGGING_RESTRICTIONS_ENABLED, true) // default
      .setProperty(PropertyKey.PROXY_S3_BUCKET_NAMING_RESTRICTIONS_ENABLED, false) // default
      .setProperty(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_ENABLED, false)
      .setProperty(
          PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_ENABLED, false) // default
      .build();

  @Rule
  public TestRule mResetRule = sResource.getResetResource();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = sResource.get().getHostname();
    mPort = sResource.get().getProxyProcess().getWebLocalPort();
    mFileSystemMaster = sResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);

    mBaseUri = String.format("%s/%s", mBaseUri, S3RestServiceHandler.SERVICE_PREFIX);

    // Assign the UFS root path "/" permissions
    Mode mode = ModeParser.parse("777");
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setMode(mode.toProto()).build();

    CreateFilePOptions filePOptions =
        CreateFilePOptions.newBuilder()
            .setRecursive(true)
            .setMode(PMode.newBuilder()
                .setOwnerBits(Bits.ALL)
                .setGroupBits(Bits.ALL)
                .setOtherBits(Bits.NONE).build())
            .setWriteType(S3RestUtils.getS3WriteType())
            .setOverwrite(true)
            .build();
    if (System.getProperty("user.name").isEmpty()) {
      sResource.setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
    } else {
      AuthenticatedClientUser.set(System.getProperty("user.name"));
    }

    mFileSystem = sResource.get().getClient();
    mFileSystem.setAttribute(new AlluxioURI("/"), options);

    mFileSystem.createDirectory(new AlluxioURI("/bucket"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder0"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder1"));

    mFileSystem.createFile(new AlluxioURI("/bucket/file0"), filePOptions);
    mFileSystem.createFile(new AlluxioURI("/bucket/file1"), filePOptions);

    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file0"), filePOptions);
    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file1"), filePOptions);
  }

  /**
   * Lists objects without parameters.
   */
  @Test
  public void listWithoutParams() throws Exception {
    //empty parameters
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults());

    assertEquals(6, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getContents().get(2).getKey());
    assertEquals("folder0/file0", expected.getContents().get(3).getKey());
    assertEquals("folder0/file1", expected.getContents().get(4).getKey());
    assertEquals("folder1/", expected.getContents().get(5).getKey());
    assertNull(expected.getCommonPrefixes());

    listStatusRestCall(NO_PARAMS, expected);
  }

  /**
   * Lists objects(v2) without parameters.
   */
  @Test
  public void listWithoutParamsV2() throws Exception {
    //empty parameters
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2));

    assertEquals(6, expected.getKeyCount().intValue());
    assertEquals(6, expected.getContents().size());

    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getContents().get(2).getKey());
    assertEquals("folder0/file0", expected.getContents().get(3).getKey());
    assertEquals("folder0/file1", expected.getContents().get(4).getKey());
    assertEquals("folder1/", expected.getContents().get(5).getKey());
    assertNull(expected.getCommonPrefixes());
    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");

    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with parameter delimiter="/".
   */
  @Test
  public void listWithDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());

    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("delimiter", AlluxioURI.SEPARATOR);  //parameters with delimiter="/"
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with delimiter="/".
   */
  @Test
  public void listWithDelimiterV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals(4, expected.getKeyCount().intValue());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());

    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);  //parameters with delimiter="/"
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with prefix="folder0".
   */
  @Test
  public void listWithPrefix() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
//    TODO(dongxinran): alluxio s3 service response is wrong when prefix is 'folder0/'
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("folder0"));

    assertEquals("folder0", expected.getPrefix());
    assertEquals(3, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "folder0");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with empty prefix.
   */
  @Test
  public void listWithEmptyPrefix() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
//    TODO(dongxinran): alluxio s3 service response is wrong when prefix is 'folder0/'
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix(""));

    assertEquals("", expected.getPrefix());
    assertEquals(6, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with non-existent prefix.
   */
  @Test
  public void listWithNonExistentPrefix() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("aa"));

    assertEquals("aa", expected.getPrefix());
    assertEquals(0, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "aa");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with prefix="folder".
   */
  @Test
  public void listWithPrefixV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("folder"));

    assertEquals("folder", expected.getPrefix());
    assertEquals(4, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertEquals("folder1/", expected.getContents().get(3).getKey());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");

    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with empty prefix.
   */
  @Test
  public void listWithEmptyPrefixV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix(""));

    assertEquals("", expected.getPrefix());
    assertEquals(6, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "");

    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with non-existent prefix.
   */
  @Test
  public void listWithNonExistentPrefixV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("file1/"));

    assertEquals("file1/", expected.getPrefix());
    assertEquals(0, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "file1/");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with prefix="f" and delimiter="/".
   */
  @Test
  public void listWithPrefixAndDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("f").setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("f", expected.getPrefix());
    assertEquals(AlluxioURI.SEPARATOR, expected.getDelimiter());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "f");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with prefix="f" and max-keys="3".
   */
  @Test
  public void listWithPrefixAndMaxKeys() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("f").setMaxKeys(3));

    assertTrue(expected.isTruncated());
    assertEquals("f", expected.getPrefix());
    assertEquals(3, expected.getMaxKeys());
    assertEquals(3, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getContents().get(2).getKey());
    assertEquals("folder0/", expected.getNextMarker());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "f");
    parameters.put("max-keys", "3");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with prefix="folder" and delimiter="/".
   */
  @Test
  public void listWithPrefixAndDelimiterV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("folder")
            .setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("folder", expected.getPrefix());
    assertEquals(AlluxioURI.SEPARATOR, expected.getDelimiter());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());
    assertEquals(0, expected.getContents().size());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);  //parameters with delimiter="/"
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with prefix="folder" and max-keys="2".
   */
  @Test
  public void listWithPrefixAndMaxKeysV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("folder").setMaxKeys(2));

    assertTrue(expected.isTruncated());
    assertEquals("folder", expected.getPrefix());
    assertEquals(2, expected.getMaxKeys());
    assertEquals(2, expected.getKeyCount().intValue());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals(ListBucketResult.encodeToken("folder0/file0"),
        expected.getNextContinuationToken());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");
    parameters.put("max-keys", "2");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with marker="file1".
   */
  @Test
  public void listWithMarker() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMarker("file1"));

    assertEquals("file1", expected.getMarker());
    assertNull(expected.getNextMarker());
    assertEquals(4, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertEquals("folder1/", expected.getContents().get(3).getKey());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("marker", "file1");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with prefix="folder0/f" and marker="abc".
   */
  @Test
  public void listWithPrefixAndMarker() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("folder0/f").setMarker("abc"));

    assertEquals("abc", expected.getMarker());
    assertEquals("folder0/f", expected.getPrefix());
    assertEquals(2, expected.getContents().size());
    assertEquals("folder0/file0", expected.getContents().get(0).getKey());
    assertEquals("folder0/file1", expected.getContents().get(1).getKey());
    assertNull(expected.getCommonPrefixes());
    assertNull(expected.getNextMarker());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("marker", "abc");
    parameters.put("prefix", "folder0/f");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with start-after="fa" and prefix="folder0".
   */
  @Test
  public void listWithPrefixAndStartAfter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
//    TODO(dongxinran): alluxio s3 service response is wrong when prefix is 'folder0/'
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("folder0").setStartAfter("fa").setListType(2));

    assertEquals("fa", expected.getStartAfter());
    assertEquals("folder0", expected.getPrefix());
    assertEquals(3, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertNull(expected.getCommonPrefixes());
    assertNull(expected.getNextMarker());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("start-after", "fa");
    parameters.put("prefix", "folder0");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with continuation-token=encodeToken("file0").
   */
  @Test
  public void listWithContinuationToken() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected1 = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1).setListType(2));
    String priorContinuationToken = expected1.getNextContinuationToken();
    ListBucketResult expected2 = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults()
            .setListType(2).setContinuationToken(priorContinuationToken));

    assertEquals(ListBucketResult.encodeToken("file0"), priorContinuationToken);
    assertEquals(priorContinuationToken, expected2.getContinuationToken());
    assertEquals(5, expected2.getContents().size());
    assertEquals(5, expected2.getKeyCount().intValue());
    assertNull(expected2.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("continuation-token", priorContinuationToken);

    listStatusRestCall(parameters, expected2);
  }

  /**
   * Lists objects(v2) with start-after="file0".
   */
  @Test
  public void listWithStartAfter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setStartAfter("file0"));

    assertEquals("file0", expected.getStartAfter());
    assertEquals(5, expected.getContents().size());
    assertEquals(5, expected.getKeyCount().intValue());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertEquals("folder0/", expected.getContents().get(1).getKey());
    assertEquals("folder0/file0", expected.getContents().get(2).getKey());
    assertEquals("folder0/file1", expected.getContents().get(3).getKey());
    assertEquals("folder1/", expected.getContents().get(4).getKey());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("start-after", "file0");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with marker="file1" and delimiter="/".
   */
  @Test
  public void listWithMarkerAndDelimiter() throws Exception {

    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMarker("file1").setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("file1", expected.getMarker());
    assertNull(expected.getNextMarker());
    assertEquals(0, expected.getContents().size());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("marker", "file1");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with continuation-token=encodeToken("file0") and delimiter="/".
   */
  @Test
  public void listWithContinuationTokenAndDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected1 = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1).setListType(2));
    String priorContinuationToken = expected1.getNextContinuationToken();
    ListBucketResult expected2 = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults()
            .setListType(2).setContinuationToken(priorContinuationToken)
            .setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals(ListBucketResult.encodeToken("file0"), priorContinuationToken);
    assertEquals(priorContinuationToken, expected2.getContinuationToken());
    assertEquals(3, expected2.getKeyCount().intValue());
    assertEquals(1, expected2.getContents().size());
    assertEquals(2, expected2.getCommonPrefixes().size());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    parameters.put("continuation-token", priorContinuationToken);
    listStatusRestCall(parameters, expected2);
  }

  /**
   * Lists objects(v2) with start-after="file0" and delimiter="/".
   */
  @Test
  public void listWithStartAfterAndDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setStartAfter("file0")
            .setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("file0", expected.getStartAfter());
    assertEquals(3, expected.getKeyCount().intValue());
    assertEquals(1, expected.getContents().size());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("start-after", "file0");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with max-keys="10". The requested objects is not truncated.
   */
  @Test
  public void listNotTruncatedObjectsWithMaxKeys() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(10));
    assertEquals(10, expected.getMaxKeys());
    assertFalse(expected.isTruncated());
    assertEquals(6, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());
    assertNull(expected.getNextMarker());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "10");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with max-keys="10". The requested objects is not truncated.
   */
  @Test
  public void listNotTruncatedObjectsWithMaxKeysV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setMaxKeys(10));
    assertEquals(10, expected.getMaxKeys());
    assertFalse(expected.isTruncated());
    assertEquals(6, expected.getContents().size());
    assertEquals(6, expected.getKeyCount().intValue());
    assertNull(expected.getCommonPrefixes());
    assertNull(expected.getNextContinuationToken());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("max-keys", "10");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with max-keys="3". The requested objects is truncated.
   */
  @Test
  public void listTruncatedObjectsWithMaxKeys() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(3));
    assertEquals(3, expected.getMaxKeys());
    assertTrue(expected.isTruncated());
    assertEquals(3, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getContents().get(2).getKey());
    assertEquals("folder0/", expected.getNextMarker());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "3");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with max-keys="3". The requested objects is truncated.
   */
  @Test
  public void listTruncatedObjectsWithMaxKeysV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setMaxKeys(3));
    assertEquals(3, expected.getMaxKeys());
    assertTrue(expected.isTruncated());
    assertEquals(3, expected.getContents().size());
    assertEquals(3, expected.getKeyCount().intValue());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getContents().get(2).getKey());
    assertEquals(ListBucketResult.encodeToken("folder0/"), expected.getNextContinuationToken());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("max-keys", "3");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with max-keys="4" and marker="file0".
   */
  @Test
  public void listWithMaxKeysAndMarker() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMarker("file0").setMaxKeys(4));

    assertTrue(expected.isTruncated());
    assertEquals("file0", expected.getMarker());
    assertEquals(4, expected.getMaxKeys());
    assertEquals(4, expected.getContents().size());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertEquals("folder0/", expected.getContents().get(1).getKey());
    assertEquals("folder0/file0", expected.getContents().get(2).getKey());
    assertEquals("folder0/file1", expected.getContents().get(3).getKey());
    assertEquals("folder0/file1", expected.getNextMarker());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "4");
    parameters.put("marker", "file0");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with max-keys="4" and start-after="folder0/file0".
   */
  @Test
  public void listWithMaxKeysAndStartAfter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setStartAfter("folder0/file0").setMaxKeys(4));

    assertFalse(expected.isTruncated());
    assertEquals("folder0/file0", expected.getStartAfter());
    assertEquals(4, expected.getMaxKeys());
    assertEquals(2, expected.getKeyCount().intValue());
    assertEquals(2, expected.getContents().size());
    assertEquals("folder0/file1", expected.getContents().get(0).getKey());
    assertEquals("folder1/", expected.getContents().get(1).getKey());
    assertNull(expected.getNextContinuationToken());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("max-keys", "4");
    parameters.put("start-after", "folder0/file0");
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects with max-keys="3" and delimiter="/".
   */
  @Test
  public void listWithMaxKeysAndDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setDelimiter(AlluxioURI.SEPARATOR).setMaxKeys(3));

    assertTrue(expected.isTruncated());
    assertEquals(3, expected.getMaxKeys());
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder0/", expected.getNextMarker());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "3");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with max-keys="3" and delimiter="/".
   */
  @Test
  public void listWithMaxKeysAndDelimiterV2() throws Exception {

    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setDelimiter(AlluxioURI.SEPARATOR)
            .setMaxKeys(3));

    assertTrue(expected.isTruncated());
    assertEquals(3, expected.getMaxKeys());
    assertEquals(3, expected.getKeyCount().intValue());
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals(ListBucketResult.encodeToken("folder0/"), expected.getNextContinuationToken());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("max-keys", "3");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    listStatusRestCall(parameters, expected);
  }

  /**
   * Lists objects(v2) with prefix="folder" and continuation-token=encodeToken("file1").
   */
  @Test
  public void listWithPrefixAndContinuationToken() throws Exception {
  //    Gets a continuation-token which equals to encodeToken("file1").
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setMaxKeys(2));
    String priorContinuationToken = expected.getNextContinuationToken();
    ListBucketResult expected2 = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults()
            .setListType(2).setContinuationToken(priorContinuationToken)
            .setPrefix("folder"));

    assertTrue(expected.isTruncated());
    assertEquals(2, expected.getKeyCount().intValue());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals(ListBucketResult.encodeToken("file1"), priorContinuationToken);

    assertEquals(priorContinuationToken, expected2.getContinuationToken());
    assertEquals(4, expected2.getKeyCount().intValue());
    assertEquals(4, expected2.getContents().size());
    assertEquals("folder0/", expected2.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected2.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected2.getContents().get(2).getKey());
    assertEquals("folder1/", expected2.getContents().get(3).getKey());
    assertNull(expected2.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");
    parameters.put("continuation-token", priorContinuationToken);

    listStatusRestCall(parameters, expected2);
  }

  /**
   * Lists objects without authorization.
   */
  @Test
  public void listBucketUnauthorized() throws Exception {
//  TODO(dongxinran): this unit test will be completed after
//   setAttribute of createDirecotory is fixed.
  }

  /**
   * Heads a non-existent bucket and lists objects in a non-existent bucket.
   */
  @Test
  public void headAndListNonExistentBucket() throws Exception {
    // Heads a non-existent bucket.
    String bucketName = "non_existent_bucket";
    // Verifies 404 status will be returned by head non-existent bucket.
    headTestCase(bucketName).checkResponseCode(Response.Status.NOT_FOUND.getStatusCode());

    // Lists objects in a non-existent bucket.
    HttpURLConnection connection2 = new TestCase(mHostname, mPort, mBaseUri,
        bucketName, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth())
        .execute();
    // Verify 404 HTTP status & NoSuchBucket S3 error code
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection2.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection2.getErrorStream());
    Assert.assertEquals(bucketName, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }
}
