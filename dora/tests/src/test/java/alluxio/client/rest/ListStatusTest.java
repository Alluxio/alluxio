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
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.GetStatusContext;
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
import com.google.common.collect.ImmutableMap;
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
import javax.validation.constraints.NotNull;
import javax.ws.rs.HttpMethod;

public class ListStatusTest {
  private static final int DATA_SIZE = 16 * Constants.KB;
  // cannot be too large, since all block streams are open until file is closed, and may run out of
  // block worker clients.
  private static final int LARGE_DATA_SIZE = 256 * Constants.KB;

  private static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();
  private static final XmlMapper XML_MAPPER = new XmlMapper();

  private static final String TEST_USER_NAME = "testuser";
  protected static final Map<String, String> NO_PARAMS = ImmutableMap.of();

  protected String mHostname;
  protected int mPort;
  protected String mBaseUri = Constants.REST_API_PREFIX;
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
//    createDirectoryPOptions
    CreateDirectoryPOptions directoryPOptions =
        CreateDirectoryPOptions.newBuilder()
            .setMode(PMode.newBuilder()
                .setOwnerBits(Bits.ALL)
                .setGroupBits(Bits.ALL)
                .setOtherBits(Bits.NONE))
            .setWriteType(S3RestUtils.getS3WriteType())
            .build();
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

    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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

    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithDelimiter() throws Exception {
    //parameters with delimiter="/"
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
//    parameters.put("prefix", "");
//    parameters.put("list-type", "");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithDelimiterV2() throws Exception {
    //parameters with delimiter="/"
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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithPrefix() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    //parameters with prefix="folder0"
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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithEmptyPrefix() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    //parameters with prefix="folder0"
//    TODO(dongxinran): alluxio s3 service response is wrong when prefix is 'folder0/'
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix(""));

    assertEquals("", expected.getPrefix());
    assertEquals(6, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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

    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithEmptyPrefixV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    //parameters with prefix=""
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix(""));

    assertEquals("", expected.getPrefix());
    assertEquals(6, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "");

    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithPrefixAndDelimiter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("f").setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("f", expected.getPrefix());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("prefix", "f");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);  //parameters with delimiter="/"
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithPrefixAndDelimiterV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("folder")
            .setDelimiter(AlluxioURI.SEPARATOR));

    assertEquals("folder", expected.getPrefix());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());
    assertEquals(0, expected.getContents().size());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);  //parameters with delimiter="/"
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithPrefixAndMaxKeysV2() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setPrefix("folder").setMaxKeys(3));

    assertTrue(expected.isTruncated());
    assertEquals("folder", expected.getPrefix());
    assertEquals(3, expected.getMaxKeys());
    assertEquals(3, expected.getKeyCount().intValue());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertEquals(ListBucketResult.encodeToken("folder0/file1"),
        expected.getNextContinuationToken());
    assertNull(expected.getCommonPrefixes());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("list-type", "2");
    parameters.put("prefix", "folder");
    parameters.put("max-keys", "3");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithPrefixAndStartAfter() throws Exception {
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
//    TODO(xinran): alluxio s3 service responses wrongly when prefix is 'folder0/'
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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected2);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listWithMarkerAndDelimiter() throws Exception {
    //parameters with list-type=2 marker="file1"
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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected2);
  }

  @Test
  public void listWithStartAfterAndDelimiter() throws Exception {
    //parameters with list-type=2 start-after="file0"
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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

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
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketUnauthorized() throws Exception {
//  TODO(dongxinran): this unit test will be completed after
//   setAttribute of createDirecotory is fixed.
  }

  @Test
  public void listNonExistentBucket() throws Exception {
//    the bucket name should never be used in other unit tests
//    to ensure the bucket path cache doesn't have this bucket name
    String bucketName = "non_existent_bucket";

    // Verify 404 HTTP status & NoSuchBucket S3 error code
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        bucketName, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .execute();
    Assert.assertEquals(404, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(bucketName, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  private TestCaseOptions getDefaultOptionsWithAuth() {
    return getDefaultOptionsWithAuth("testuser");
  }

  private TestCaseOptions getDefaultOptionsWithAuth(@NotNull String user) {
    TestCaseOptions options = TestCaseOptions.defaults();
    options.setAuthorization("AWS4-HMAC-SHA256 Credential=" + user + "/20220830");
    return options;
  }
}
