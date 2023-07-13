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
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.proxy.s3.CompleteMultipartUploadRequest;
import alluxio.proxy.s3.CompleteMultipartUploadResult;
import alluxio.proxy.s3.InitiateMultipartUploadResult;
import alluxio.proxy.s3.ListAllMyBucketsResult;
import alluxio.proxy.s3.ListBucketOptions;
import alluxio.proxy.s3.ListBucketResult;
import alluxio.proxy.s3.ListMultipartUploadsResult;
import alluxio.proxy.s3.ListPartsResult;
import alluxio.proxy.s3.S3Constants;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3RestServiceHandler;
import alluxio.proxy.s3.S3RestUtils;
import alluxio.proxy.s3.TaggingData;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.ModeParser;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.net.HttpURLConnection;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

/**
 * Test cases for {@link alluxio.proxy.s3.S3RestServiceHandler}.
 */
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "yuyang",
    comment = "fix the tests")
@Ignore
public final class S3ClientRestApiTest extends RestApiTest {
  private static final int DATA_SIZE = 16 * Constants.KB;
  // cannot be too large, since all block streams are open until file is closed, and may run out of
  // block worker clients.
  private static final int LARGE_DATA_SIZE = 256 * Constants.KB;

  private static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();
  private static final XmlMapper XML_MAPPER = new XmlMapper();

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
    mFileSystem = sResource.get().getClient();
    mBaseUri = String.format("%s/%s", mBaseUri, S3RestServiceHandler.SERVICE_PREFIX);

    // Assign the UFS root path "/" permissions
    Mode mode = ModeParser.parse("777");
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setMode(mode.toProto()).build();
    mFileSystem.setAttribute(new AlluxioURI("/"), options);

    if (System.getProperty("user.name").isEmpty()) {
      sResource.setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
    } else {
      AuthenticatedClientUser.set(System.getProperty("user.name"));
    }
  }

  @Deprecated
  public void listAllMyBuckets() throws Exception {
    // TODO(Jackson-Wang-7): need to be reimplemented as metadata no longer exists in master
    AlluxioURI bucket0Path = new AlluxioURI("/bucket0");
    FileSystem fs0 = S3RestUtils.createFileSystemForUser("user0", mFileSystem);
    fs0.createDirectory(bucket0Path);
    SetAttributePOptions setAttributeOptions =
        SetAttributePOptions.newBuilder().setOwner("user0").build();
    mFileSystem.setAttribute(new AlluxioURI("/bucket0"), setAttributeOptions);
    URIStatus bucket0Status = fs0.getStatus(bucket0Path);

    AlluxioURI bucket1Path = new AlluxioURI("/bucket1");
    FileSystem fs1 = S3RestUtils.createFileSystemForUser("user1", mFileSystem);
    fs1.createDirectory(bucket1Path);
    setAttributeOptions = SetAttributePOptions.newBuilder().setOwner("user1").build();
    mFileSystem.setAttribute(new AlluxioURI("/bucket1"), setAttributeOptions);
    URIStatus bucket1Status = fs1.getStatus(bucket1Path);

    TestCaseOptions requestOptions = getDefaultOptionsWithAuth("user0")
        .setContentType(TestCaseOptions.XML_CONTENT_TYPE);
    ListAllMyBucketsResult expected = new ListAllMyBucketsResult(Lists.newArrayList(bucket0Status));
    new TestCase(mHostname, mPort, mBaseUri,
        "", NO_PARAMS, HttpMethod.GET,
        requestOptions).runAndCheckResult(expected);

    expected = new ListAllMyBucketsResult(Lists.newArrayList(bucket1Status));
    requestOptions = getDefaultOptionsWithAuth("user1")
        .setContentType(TestCaseOptions.XML_CONTENT_TYPE);
    new TestCase(mHostname, mPort, mBaseUri,
        "", NO_PARAMS, HttpMethod.GET,
        requestOptions).runAndCheckResult(expected);

    // Verify 400 HTTP status & AuthorizationHeaderMalformed S3 error code for empty "Authorization"
    requestOptions.setAuthorization("");
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        "", NO_PARAMS, HttpMethod.GET,
        requestOptions).execute();
    Assert.assertEquals(400, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(response.getCode(), S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED.getCode());
  }

  @Test
  public void listBucket() throws Exception {
    mFileSystem.createDirectory(new AlluxioURI("/bucket"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder0"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder1"));

    mFileSystem.createFile(new AlluxioURI("/bucket/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/file1"));

    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file1"));

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

    //parameters with delimiter="/"
    List<URIStatus> delimStatuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(false).build());

    expected = new ListBucketResult("bucket", delimStatuses,
        ListBucketOptions.defaults().setDelimiter(AlluxioURI.SEPARATOR));
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals(2, expected.getCommonPrefixes().size());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("folder1/", expected.getCommonPrefixes().get(1).getPrefix());

    Map<String, String> parameters = new HashMap<>();
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with prefix="folder0"
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("folder0"));
    assertEquals(3, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertEquals("folder0/file0", expected.getContents().get(1).getKey());
    assertEquals("folder0/file1", expected.getContents().get(2).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.clear();
    parameters.put("prefix", "folder0");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with list-type=2 start-after="file0"
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setListType(2).setStartAfter("file0"));
    assertEquals(5, expected.getContents().size());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertEquals("folder0/", expected.getContents().get(1).getKey());
    assertEquals("folder0/file0", expected.getContents().get(2).getKey());
    assertEquals("folder0/file1", expected.getContents().get(3).getKey());
    assertEquals("folder1/", expected.getContents().get(4).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.clear();
    parameters.put("list-type", "2");
    parameters.put("start-after", "file0");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketUnauthorized() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        bucket, NO_PARAMS, HttpMethod.GET, getDefaultOptionsWithAuth("dummy")).execute();
    Assert.assertEquals(403, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(response.getCode(), S3ErrorCode.ACCESS_DENIED_ERROR.getCode());
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

  @Test
  public void listBucketCommonPrefixes() throws Exception {
    AlluxioURI uri = new AlluxioURI("/bucket");
    mFileSystem.createDirectory(uri);
    mFileSystem.createDirectory(new AlluxioURI("/bucket/c_first_folder"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/d_next_folder"));

    mFileSystem.createFile(new AlluxioURI("/bucket/a_first_file"));
    mFileSystem.createFile(new AlluxioURI("/bucket/b_next_file"));

    mFileSystem.createFile(new AlluxioURI("/bucket/c_first_folder/file"));
    mFileSystem.createFile(new AlluxioURI("/bucket/d_next_folder/file"));

    mFileSystem.createFile(new AlluxioURI("/bucket/z_last_file"));

    List<URIStatus> delimStatuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(false).build());

    //parameters with max-keys=3
    ListBucketResult expected = new ListBucketResult("bucket", delimStatuses,
        ListBucketOptions.defaults().setMaxKeys(3).setDelimiter(AlluxioURI.SEPARATOR));
    String nextMarker = expected.getNextMarker();
    assertEquals(3, expected.getMaxKeys());
    assertTrue(expected.isTruncated());
    assertEquals("c_first_folder/", nextMarker);
    assertEquals(2, expected.getContents().size());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("a_first_file", expected.getContents().get(0).getKey());
    assertEquals("b_next_file", expected.getContents().get(1).getKey());
    assertEquals("c_first_folder/" , expected.getCommonPrefixes().get(0).getPrefix());

    final Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "3");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //subsequent request using next-marker
    expected = new ListBucketResult("bucket", delimStatuses,
        ListBucketOptions.defaults().setMarker(nextMarker).setDelimiter(AlluxioURI.SEPARATOR));
    assertFalse(expected.isTruncated());
    assertNull(expected.getNextMarker());
    assertEquals(1, expected.getContents().size());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("d_next_folder/" , expected.getCommonPrefixes().get(0).getPrefix());
    assertEquals("z_last_file", expected.getContents().get(0).getKey());

    parameters.remove("max-keys");
    parameters.put("marker", nextMarker);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketPagination() throws Exception {
    AlluxioURI uri = new AlluxioURI("/bucket");
    mFileSystem.createDirectory(uri);
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder0"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder1"));

    mFileSystem.createFile(new AlluxioURI("/bucket/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/file1"));

    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file1"));

    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    //parameters with max-keys=1
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1));
    assertNull(expected.getContinuationToken()); // only used in V2 API
    assertNull(expected.getStartAfter()); // only used in V2 API
    String priorMarker;
    String nextMarker = expected.getNextMarker();
    assertEquals("", expected.getMarker());
    assertEquals("file0", nextMarker);
    assertNull(expected.getKeyCount());
    assertEquals(1, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    final Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "1");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    priorMarker = nextMarker;

    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1).setMarker(nextMarker));
    nextMarker = expected.getNextMarker();
    assertEquals(priorMarker, expected.getMarker());
    assertNull(expected.getKeyCount());
    assertEquals(1, expected.getContents().size());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.put("marker", priorMarker);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    priorMarker = nextMarker;

    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1).setMarker(nextMarker));
    nextMarker = expected.getNextMarker();
    assertEquals(priorMarker, expected.getMarker());
    assertNull(expected.getKeyCount());
    assertEquals(1, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.put("marker", priorMarker);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with list-type=2 and max-key=1
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1).setListType(2));
    assertNull(expected.getMarker()); // we only use ContinuationToken / StartAfter for V2
    String priorContinuationToken;
    String nextContinuationToken = expected.getNextContinuationToken();
    assertNull(expected.getContinuationToken());
    assertEquals(ListBucketResult.encodeToken("file0"), nextContinuationToken);
    assertEquals(1, expected.getKeyCount().intValue());
    assertEquals(1, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.clear();
    parameters.put("max-keys", "1");
    parameters.put("list-type", "2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    priorContinuationToken = nextContinuationToken;

    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1)
            .setListType(2).setContinuationToken(nextContinuationToken));
    nextContinuationToken = expected.getNextContinuationToken();
    assertEquals(priorContinuationToken, expected.getContinuationToken());
    assertEquals(ListBucketResult.encodeToken("file1"), nextContinuationToken);
    assertEquals(1, expected.getKeyCount().intValue());
    assertEquals(1, expected.getContents().size());
    assertEquals("file1", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.put("continuation-token", priorContinuationToken);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    priorContinuationToken = nextContinuationToken;

    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(1)
            .setListType(2).setContinuationToken(nextContinuationToken));
    nextContinuationToken = expected.getNextContinuationToken();
    assertEquals(priorContinuationToken, expected.getContinuationToken());
    assertEquals(ListBucketResult.encodeToken("folder0/"), nextContinuationToken);
    assertEquals(1, expected.getKeyCount().intValue());
    assertEquals(1, expected.getContents().size());
    assertEquals("folder0/", expected.getContents().get(0).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.put("continuation-token", priorContinuationToken);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketExactlyMaxKeys() throws Exception {
    AlluxioURI uri = new AlluxioURI("/bucket");
    mFileSystem.createDirectory(uri);
    mFileSystem.createFile(new AlluxioURI("/bucket/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/file1"));
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    // ListObjects v1
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(2));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter()); // only used in V2 API
    assertNull(expected.getContinuationToken()); // only used in V2 API
    assertNull(expected.getNextContinuationToken()); // only used in V2 API
    assertEquals("", expected.getMarker());
    assertNull(expected.getNextMarker());
    assertNull(expected.getKeyCount()); // only used in V2 API
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertNull(expected.getCommonPrefixes());

    final Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    // ListObjectsV2
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(2).setListType(2));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter());
    assertNull(expected.getContinuationToken());
    assertNull(expected.getNextContinuationToken());
    assertNull(expected.getMarker()); // only used in V1 API
    assertNull(expected.getNextMarker()); // only used in V1 API
    assertEquals(2, expected.getKeyCount().intValue());
    assertEquals(2, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertNull(expected.getCommonPrefixes());

    parameters.put("list-type", "2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
    parameters.remove("list-type");

    // Create a directory to test with common prefixes
    mFileSystem.createDirectory(new AlluxioURI("/bucket/folder0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/folder0/file1"));

    List<URIStatus> delimStatuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(false).build());

    // ListObjects v1
    expected = new ListBucketResult("bucket", delimStatuses,
        ListBucketOptions.defaults().setMaxKeys(3).setDelimiter(AlluxioURI.SEPARATOR));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter()); // only used in V2 API
    assertNull(expected.getContinuationToken()); // only used in V2 API
    assertNull(expected.getNextContinuationToken()); // only used in V2 API
    assertEquals("", expected.getMarker());
    assertNull(expected.getNextMarker());
    assertNull(expected.getKeyCount()); // only used in V2 API
    assertEquals(AlluxioURI.SEPARATOR, expected.getDelimiter());
    assertEquals(2, expected.getContents().size());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());

    parameters.put("max-keys", "3");
    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    // ListObjectsV2
    expected = new ListBucketResult("bucket", delimStatuses,
        ListBucketOptions.defaults().setMaxKeys(3).setDelimiter(AlluxioURI.SEPARATOR)
            .setListType(2));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter());
    assertNull(expected.getContinuationToken());
    assertNull(expected.getNextContinuationToken());
    assertNull(expected.getMarker()); // only used in V1 API
    assertNull(expected.getNextMarker()); // only used in V1 API
    assertEquals(3, expected.getKeyCount().intValue());
    assertEquals(2, expected.getContents().size());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file1", expected.getContents().get(1).getKey());
    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());

    parameters.put("list-type", "2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketZeroMaxKeys() throws Exception {
    AlluxioURI uri = new AlluxioURI("/bucket");
    mFileSystem.createDirectory(uri);
    mFileSystem.createFile(new AlluxioURI("/bucket/file0"));
    mFileSystem.createFile(new AlluxioURI("/bucket/file1"));
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    // ListObjects v1
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(0));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter()); // only used in V2 API
    assertNull(expected.getContinuationToken()); // only used in V2 API
    assertNull(expected.getNextContinuationToken()); // only used in V2 API
    assertEquals("", expected.getMarker());
    assertNull(expected.getNextMarker());
    assertNull(expected.getKeyCount()); // only used in V2 API
    assertEquals(0, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    final Map<String, String> parameters = new HashMap<>();
    parameters.put("max-keys", "0");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    // ListObjectsV2
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setMaxKeys(0).setListType(2));
    assertFalse(expected.isTruncated());
    assertNull(expected.getStartAfter());
    assertNull(expected.getContinuationToken());
    assertNull(expected.getNextContinuationToken());
    assertNull(expected.getMarker()); // only used in V1 API
    assertNull(expected.getNextMarker()); // only used in V1 API
    assertEquals(0, expected.getKeyCount().intValue());
    assertEquals(0, expected.getContents().size());
    assertNull(expected.getCommonPrefixes());

    parameters.put("list-type", "2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void listBucketPrefixZeroMatches() throws Exception {
    // Test ListObjects with an empty bucket
    mFileSystem.createDirectory(new AlluxioURI("/bucket"));
    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    //parameters with no prefix
    ListBucketResult expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults());
    assertEquals(0, expected.getContents().size());

    final Map<String, String> parameters = new HashMap<>();
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with prefix=""
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix(""));
    assertEquals(0, expected.getContents().size());

    parameters.put("prefix", "");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    // Test ListObjects with objects in the bucket
    mFileSystem.createFile(new AlluxioURI("/bucket/file0"));
    mFileSystem.createDirectory(new AlluxioURI("/bucket/file_store"));
    mFileSystem.createFile(new AlluxioURI("/bucket/file_store/file1"));
    statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());

    //parameters with no prefix
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults());
    assertEquals(3, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file_store/", expected.getContents().get(1).getKey());
    assertEquals("file_store/file1", expected.getContents().get(2).getKey());

    parameters.remove("prefix");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with prefix=""
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix(""));
    assertEquals(3, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals("file_store/", expected.getContents().get(1).getKey());
    assertEquals("file_store/file1", expected.getContents().get(2).getKey());

    parameters.put("prefix", "");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with non-existent prefix="dne_folder/file"
    try {
      expected = new ListBucketResult("bucket", statuses,
          ListBucketOptions.defaults().setPrefix("dne_folder/file"));
    } catch (Exception e) {
      // expected
      // TODO(czhu): with the current implementation of prefixes w/o delimiters, there is
      // never a FileDoesNotExistException because we just list the entire bucket recursively
      statuses = new ArrayList<>();
      return;
    }
    assertEquals(0, expected.getContents().size());

    parameters.put("prefix", "dne_folder/file");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    // prefix test with delimiter="/"
    statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
        ListStatusPOptions.newBuilder().setRecursive(false).build());

    //parameters with prefix="file"
    expected = new ListBucketResult("bucket", statuses,
        ListBucketOptions.defaults().setPrefix("file").setDelimiter(AlluxioURI.SEPARATOR));
    assertEquals(1, expected.getContents().size());
    assertEquals("file0", expected.getContents().get(0).getKey());
    assertEquals(1, expected.getCommonPrefixes().size());
    assertEquals("file_store/", expected.getCommonPrefixes().get(0).getPrefix());

    parameters.put("delimiter", AlluxioURI.SEPARATOR);
    parameters.put("prefix", "file");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with prefix="file_store/file1/"
    try {
      statuses = mFileSystem.listStatus(new AlluxioURI("/bucket/file_store/file1/"),
          ListStatusPOptions.newBuilder().setRecursive(false).build());
    } catch (Exception e) {
      // expected
      // TODO(czhu): with the current implementation of listStatus() the trailing '/' character
      // doesn't cause a failure, despite `/bucket/file_store/file1` not being a directory
      statuses = new ArrayList<>();
      return;
    }
    expected = new ListBucketResult("bucket", statuses, ListBucketOptions.defaults()
        .setPrefix("file_store/file1/").setDelimiter(AlluxioURI.SEPARATOR));
    assertEquals(0, expected.getContents().size());
    assertEquals(0, expected.getCommonPrefixes().size());

    parameters.put("prefix", "file_store/file1/");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);

    //parameters with prefix="file_store/file2"
    try {
      statuses = mFileSystem.listStatus(new AlluxioURI("/bucket/file_store/file2"),
          ListStatusPOptions.newBuilder().setRecursive(false).build());
    } catch (FileDoesNotExistException e) {
      // expected
      statuses = new ArrayList<>();
      return;
    }
    expected = new ListBucketResult("bucket", statuses, ListBucketOptions.defaults()
        .setPrefix("file_store/file2").setDelimiter(AlluxioURI.SEPARATOR));
    assertEquals(0, expected.getContents().size());
    assertEquals(0, expected.getCommonPrefixes().size());

    parameters.put("prefix", "file_store/file2");
    new TestCase(mHostname, mPort, mBaseUri,
        "bucket", parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  @Test
  public void headBucket() throws Exception {
    final String bucket = "bucket-to-head";
    final String nonExistingBucket = "non-existing-bucket";
    createBucketRestCall(bucket);
    // Verify the directory is created for the new bucket
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    Assert.assertTrue(mFileSystemMaster
        .listStatus(uri, ListStatusContext.defaults()).isEmpty());

    HttpURLConnection connection = headBucketRestCall(bucket);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), connection.getResponseCode());

    // Verify 404 status will be returned by head none existing bucket.
    connection = headBucketRestCall(nonExistingBucket);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
  }

  private void putBucket(String bucket) throws Exception {
    putBucket(bucket, TEST_USER_NAME);
  }

  private void putBucket(String bucket, String user) throws Exception {
    createBucketRestCall(bucket, user);
    // Verify the directory is created for the new bucket.
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    Assert.assertTrue(mFileSystemMaster
        .listStatus(uri, ListStatusContext.defaults()).isEmpty());
  }

  @Test
  public void putBucket() throws Exception {
    putBucket("bucket", "dummy");
  }

  @Test
  public void putExistsBucket() throws Exception {
    String bucket = "bucket";
    putBucket(bucket);

    TestCaseOptions options = getDefaultOptionsWithAuth("dummy");
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        bucket, NO_PARAMS, HttpMethod.PUT, options)
        .execute();
    Assert.assertEquals(Response.Status.CONFLICT.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(bucket, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.BUCKET_ALREADY_EXISTS, response.getCode());
  }

  @Test
  public void deleteBucket() throws Exception {
    String bucket = "bucket_to_delete";
    putBucket(bucket);

    HttpURLConnection connection = deleteBucketRestCall(bucket);
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), connection.getResponseCode());

    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI(AlluxioURI.SEPARATOR + bucket),
          GET_STATUS_CONTEXT);
    } catch (FileDoesNotExistException e) {
      // expected
      return;
    }
    Assert.fail("bucket should have been removed");
  }

  @Test
  public void deleteNonExistingBucket() throws Exception {
    final String bucketName = "non-existing-bucket";

    try {
      // Delete a non-existing bucket should fail.
      deleteBucketRestCall(bucketName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete a non-existing bucket should fail");
  }

  @Test
  public void getNonExistingBucket() throws Exception {
    final String bucketName = "root-level-file";
    mFileSystem.createFile(new AlluxioURI("/" + bucketName));

    try {
      // GET on a non-existing bucket should fail.
      new TestCase(mHostname, mPort, mBaseUri,
          bucketName, NO_PARAMS, HttpMethod.GET,
          getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
          .runAndGetResponse();
    } catch (AssertionError e) {
      return; // expected
    }
    Assert.fail("GET on a non-existing bucket should fail");
  }

  @Test
  public void deleteNonEmptyBucket() throws Exception {
    final String bucketName = "non-empty-bucket";

    createBucketRestCall(bucketName);

    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI fileUri = new AlluxioURI(uri.getPath() + "/file");
    mFileSystemMaster.createFile(fileUri, CreateFileContext.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(mFileSystemMaster
        .listStatus(uri, ListStatusContext.defaults()).isEmpty());

    try {
      // Delete a non-empty bucket should fail.
      deleteBucketRestCall(bucketName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete a non-empty bucket should fail");
  }

  private void createObject(String objectKey, byte[] object, String uploadId,
      Integer partNumber) throws Exception {
    Map<String, String> params = new HashMap<>();
    if (uploadId != null) {
      params.put("uploadId", uploadId);
    }
    if (partNumber != null) {
      params.put("partNumber", partNumber.toString());
    }
    createObjectRestCall(objectKey, params,
        getDefaultOptionsWithAuth()
            .setBody(object)
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(object)));
  }

  private void putObjectTest(String bucket, String objectKey, byte[] object, String uploadId,
      Integer partNumber) throws Exception {
    final String fullObjectKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    createObject(fullObjectKey, object, uploadId, partNumber);

    // Verify the object is created for the new bucket.
    AlluxioURI bucketURI = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    AlluxioURI objectURI = new AlluxioURI(AlluxioURI.SEPARATOR + fullObjectKey);
    if (uploadId != null) {
      String tmpDir = S3RestUtils.getMultipartTemporaryDirForObject(
          bucketURI.getPath(), objectKey, uploadId);
      bucketURI = new AlluxioURI(tmpDir);
      objectURI = new AlluxioURI(tmpDir + AlluxioURI.SEPARATOR + partNumber.toString());
    }
    List<FileInfo> fileInfos =
        mFileSystemMaster.listStatus(bucketURI, ListStatusContext.defaults());
    Assert.assertEquals(1, fileInfos.size());
    Assert.assertEquals(objectURI.getPath(), fileInfos.get(0).getPath());

    // Verify the object's content.
    FileInStream is = mFileSystem.openFile(objectURI);
    byte[] writtenObjectContent = IOUtils.toString(is).getBytes();
    is.close();
    Assert.assertArrayEquals(object, writtenObjectContent);
    Assert.assertNotNull(fileInfos.get(0).getXAttr());
    Assert.assertEquals(
        Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(writtenObjectContent)),
        new String(fileInfos.get(0).getXAttr().get(S3Constants.ETAG_XATTR_KEY),
            S3Constants.XATTR_STR_CHARSET));
  }

  @Test
  public void testGetDeletedObject() throws Exception {
    String bucket = "bucket";
    String objectKey = "object";
    String object = CommonUtils.randomAlphaNumString(DATA_SIZE);
    final String fullObjectKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    AlluxioURI bucketURI = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    AlluxioURI objectURI = new AlluxioURI(AlluxioURI.SEPARATOR + fullObjectKey);

    createBucketRestCall(bucket);
    FileOutStream outStream = mFileSystem.createFile(objectURI,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build());
    outStream.write(object.getBytes());
    outStream.close();

    // free the object in alluxio and delete it in UFS.
    mFileSystem.free(objectURI, FreePOptions.newBuilder().setForced(true).build());
    FileUtils.deleteQuietly(
        new File(sResource.get().getAlluxioHome() + "/underFSStorage/" + fullObjectKey));

    // Verify the object is existent in the alluxio.
    List<FileInfo> fileInfos =
        mFileSystemMaster.listStatus(bucketURI, ListStatusContext.defaults());
    Assert.assertEquals(1, fileInfos.size());
    Assert.assertEquals(objectURI.getPath(), fileInfos.get(0).getPath());

    // Verify 404 status will be returned by Getting Object
    HttpURLConnection connection = getObjectRestCallWithError(fullObjectKey);
    Assert.assertEquals(404, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals("", response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_KEY, response.getCode());
  }

  @Test
  public void putObjectToDeletedBucket() throws Exception {
    String object = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createBucketRestCall("bucket");
    // delete the bucket in alluxio and UFS, but the bucket remains in BUCKET_PATH_CACHE
    mFileSystem.delete(new AlluxioURI("/bucket"));
    // put object to non-existent bucket
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        "bucket/object", NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .setBody(object.getBytes())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(object.getBytes())))
        .execute();

    Assert.assertEquals(404, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void putDirectoryToDeletedBucket() throws Exception {
    createBucketRestCall("bucket");
    // delete the bucket in alluxio and UFS, but the bucket remains in BUCKET_PATH_CACHE
    mFileSystem.delete(new AlluxioURI("/bucket"));
    // put directory to non-existent bucket
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        "bucket/directory/", NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .setBody(new byte[] {})
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(new byte[] {})))
        .execute();

    Assert.assertEquals(404, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void putDirectoryObject() throws Exception {
    final String bucketName = "directory-bucket";
    createBucketRestCall(bucketName);

    final String directoryName = "directory/";
    createObject(bucketName + AlluxioURI.SEPARATOR + directoryName, new byte[]{}, null, null);

    final List<URIStatus> statuses = mFileSystem.listStatus(
        new AlluxioURI(AlluxioURI.SEPARATOR + bucketName));

    assertEquals(1, statuses.size());
    assertEquals(true, statuses.get(0).isFolder());
  }

  @Test
  public void putSmallObject() throws Exception {
    final String bucketName = "small-object-bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    putObjectTest(bucketName, objectName, "Hello World!".getBytes(), null, null);
  }

  @Test
  public void putLargeObject() throws Exception {
    final String bucketName = "large-object-bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    final byte[] object = CommonUtils.randomAlphaNumString(LARGE_DATA_SIZE).getBytes();
    putObjectTest(bucketName, objectName, object, null, null);
  }

  @Test
  public void putObjectUnderNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    String message = "hello world";
    try {
      createObjectRestCall(objectKey, NO_PARAMS,
          getDefaultOptionsWithAuth()
              .setBody(message.getBytes())
              .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
              .setMD5(computeObjectChecksum(message.getBytes())));
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create object under non-existent bucket should fail");
  }

  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    String objectContent = "hello world";
    try {
      String wrongMD5 = BaseEncoding.base64().encode(objectContent.getBytes());
      createObjectRestCall(objectKey, NO_PARAMS,
          getDefaultOptionsWithAuth()
              .setBody(objectContent.getBytes())
              .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
              .setMD5(wrongMD5));
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create object with wrong Content-MD5 should fail");
  }

  @Test
  public void putObjectWithNoMD5() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    String objectContent = "no md5 set";
    TestCaseOptions options = getDefaultOptionsWithAuth();
    options.setBody(objectContent.getBytes());
    options.setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE);
    new TestCase(mHostname, mPort, mBaseUri,
        objectKey, NO_PARAMS, HttpMethod.PUT,
        options).runAndCheckResult();
  }

  private void getObjectTest(byte[] expectedObject) throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(expectedObject)
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(expectedObject)));
    Assert.assertArrayEquals(expectedObject, getObjectRestCall(objectKey).getBytes());
  }

  @Test
  public void getSmallObject() throws Exception {
    getObjectTest("Hello World!".getBytes());
  }

  @Test
  public void getLargeObject() throws Exception {
    getObjectTest(CommonUtils.randomAlphaNumString(LARGE_DATA_SIZE).getBytes());
  }

  @Test
  public void getNonExistentObject() throws Exception {
    final String objectKey = "bucket/non-existent-object";
    try {
      getObjectRestCall(objectKey);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("get non-existent object should fail");
  }

  @Test
  public void getUnauthorizedObject() throws Exception {
    putBucket("bucket");
    createObject("bucket/object", "Hello World!".getBytes(), null, null);

    TestCaseOptions options = getDefaultOptionsWithAuth("unauthorized");
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        "bucket/object", NO_PARAMS, HttpMethod.GET,
        options).execute();
    Assert.assertEquals(403, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.ACCESS_DENIED_ERROR, response.getCode());
  }

  @Test
  public void getObjectMetadata() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    final byte[] objectContent = CommonUtils.randomAlphaNumString(10).getBytes();
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectContent)
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectContent)));

    HttpURLConnection connection = getObjectMetadataRestCall(objectKey);
    URIStatus status = mFileSystem.getStatus(
        new AlluxioURI(AlluxioURI.SEPARATOR + objectKey));
    // remove the milliseconds from the last modification time because the accuracy of HTTP dates
    // is up to seconds.
    long lastModified = status.getLastModificationTimeMs() / 1000 * 1000;
    Assert.assertEquals(lastModified, connection.getLastModified());
    Assert.assertEquals(String.valueOf(status.getLength()),
        connection.getHeaderField(S3Constants.S3_CONTENT_LENGTH_HEADER));
  }

  @Test
  public void getNonExistentObjectMetadata() throws Exception {
    final String objectKey = "bucket/non-existent-object";
    try {
      getObjectMetadataRestCall(objectKey);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("get metadata of non-existent object should fail");
  }

  @Test
  public void deleteObject() throws Exception {
    final String bucketName = "bucket-with-object-to-delete";
    createBucketRestCall(bucketName);

    final String objectName = "file";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI fileUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createFile(fileUri, CreateFileContext.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(mFileSystemMaster
        .listStatus(bucketUri, ListStatusContext.defaults()).isEmpty());

    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);

    // Verify the object is deleted.
    Assert.assertTrue(mFileSystemMaster
        .listStatus(bucketUri, ListStatusContext.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioEmptyDir() throws Exception {
    final String bucketName = "bucket-with-empty-dir-to-delete";
    createBucketRestCall(bucketName);

    String objectName = "empty-dir/";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI dirUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryContext.defaults());

    // Verify the directory is created for the new bucket, and empty-dir is created under it.
    Assert.assertFalse(mFileSystemMaster
        .listStatus(bucketUri, ListStatusContext.defaults()).isEmpty());

    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);

    // Verify the empty-dir as a valid object is deleted.
    Assert.assertTrue(mFileSystemMaster
        .listStatus(bucketUri, ListStatusContext.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioNonEmptyDir() throws Exception {
    final String bucketName = "bucket-with-non-empty-dir-to-delete";
    createBucketRestCall(bucketName);

    String objectName = "non-empty-dir/";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI dirUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryContext.defaults());

    mFileSystemMaster.createFile(
        new AlluxioURI(dirUri.getPath() + "/file"), CreateFileContext.defaults());

    Assert.assertFalse(mFileSystemMaster
        .listStatus(dirUri, ListStatusContext.defaults()).isEmpty());

    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);
  }

  @Test
  public void deleteNonExistingObject() throws Exception {
    final String bucketName = "bucket-with-nothing";
    createBucketRestCall(bucketName);

    String objectName = "non-existing-object";
    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);
  }

  @Test
  public void initiateMultipartUpload() throws Exception {
    final String user = "dummy";
    final String bucketName = "bucket";
    createBucketRestCall(bucketName, user);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String result = initiateMultipartUploadRestCall(objectKey, user);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();

    InitiateMultipartUploadResult expected =
        new InitiateMultipartUploadResult(bucketName, objectName, uploadId);
    String expectedResult = XML_MAPPER.writeValueAsString(expected);

    Assert.assertEquals(expectedResult, result);

    URIStatus mpMetaFileStatus = mFileSystem.getStatus(
        new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)));
    Assert.assertTrue(mpMetaFileStatus.isCompleted());

    AlluxioURI mpTempDirURI = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucketName),
        objectName, uploadId));
    Assert.assertTrue(mFileSystem.exists(mpTempDirURI));
    URIStatus mpTempDirStatus = mFileSystem.getStatus(mpTempDirURI);
    Assert.assertTrue(mpTempDirStatus.getFileInfo().isFolder());
  }

  @Test
  public void uploadPart() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);

    final String uploadId = multipartUploadResult.getUploadId();
    final byte[] object = CommonUtils.randomAlphaNumString(DATA_SIZE).getBytes();
    putObjectTest(bucketName, objectName, object, uploadId, 1);

    // overwrite an existing part
    putObjectTest(bucketName, objectName, object, uploadId, 1);
  }

  @Test
  public void uploadPartWithNonExistingUploadId() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);

    final String uploadId = multipartUploadResult.getUploadId();
    final byte[] object = CommonUtils.randomAlphaNumString(DATA_SIZE).getBytes();
    try {
      putObjectTest(bucketName, objectName, object, UUID.randomUUID().toString(), 1);
    } catch (AssertionError e) {
      // Expected because of the wrong upload ID.
      return;
    }
    Assert.fail("Upload part of an object with wrong upload ID should fail");
  }

  @Test
  public void uploadPartWithoutInitiation() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    try {
      final String objectName = "object";
      final byte[] object = CommonUtils.randomAlphaNumString(DATA_SIZE).getBytes();
      putObjectTest(bucketName, objectName, object, UUID.randomUUID().toString(), 1);
    } catch (AssertionError e) {
      // Expected because there is no such upload ID.
      return;
    }
    Assert.fail("Upload part of an object without multipart upload initialization should fail");
  }

  @Test
  public void testUploadPartCopy() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "src-object";
    String srcObjectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    final byte[] srcObjectContent = CommonUtils.randomAlphaNumString(DATA_SIZE).getBytes();
    putObjectTest(bucketName, objectName, srcObjectContent, null, null);

    // UploadPartCopy object
    String targetObjectName = "target-MP-object";
    String targetMPObjectKey = bucketName + AlluxioURI.SEPARATOR + targetObjectName;
    String result = initiateMultipartUploadRestCall(targetMPObjectKey);
    final String uploadId = XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class)
        .getUploadId();
    Map<String, String> params = new HashMap<>();
    params.put("uploadId", uploadId);
    params.put("partNumber", "1");

    new TestCase(mHostname, mPort, mBaseUri,
        targetMPObjectKey,
        params, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, srcObjectKey)).runAndGetResponse();

    List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
    partList.add(new CompleteMultipartUploadRequest.Part("", 1));
    result = completeMultipartUploadRestCall(targetMPObjectKey, uploadId,
        new CompleteMultipartUploadRequest(partList));

    // Verify the object's content.
    byte[] downloadTargetMpObj = new byte[DATA_SIZE];
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    try (FileInStream is = mFileSystem
        .openFile(new AlluxioURI("/" + targetMPObjectKey))) {
      is.read(downloadTargetMpObj, 0, DATA_SIZE);
      Assert.assertTrue(is.available() <= 0);
    }
    Assert.assertArrayEquals(srcObjectContent, downloadTargetMpObj);
  }

  @Test
  public void listParts() throws Exception {
    final String bucket = "bucket";
    final String bucketPath = AlluxioURI.SEPARATOR + bucket;
    createBucketRestCall(bucket);

    final String object = "object";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + object;

    // Initiate multipart upload to get upload ID.
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();

    // No parts are uploaded yet.
    result = listPartsRestCall(objectKey, uploadId);
    ListPartsResult listPartsResult = XML_MAPPER.readValue(result, ListPartsResult.class);
    Assert.assertEquals(bucketPath, listPartsResult.getBucket());
    Assert.assertEquals(object, listPartsResult.getKey());
    Assert.assertEquals(uploadId, listPartsResult.getUploadId());
    Assert.assertEquals(0, listPartsResult.getParts().size());

    // Upload 2 parts.
    String object1 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    String object2 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObject(objectKey, object1.getBytes(), uploadId, 1);
    createObject(objectKey, object2.getBytes(), uploadId, 2);

    result = listPartsRestCall(objectKey, uploadId);
    listPartsResult = XML_MAPPER.readValue(result, ListPartsResult.class);
    Assert.assertEquals(bucketPath, listPartsResult.getBucket());
    Assert.assertEquals(object, listPartsResult.getKey());
    Assert.assertEquals(uploadId, listPartsResult.getUploadId());

    String tmpDir = S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId);
    List<ListPartsResult.Part> parts = listPartsResult.getParts();
    Assert.assertEquals(2, parts.size());
    for (int partNumber = 1; partNumber <= parts.size(); partNumber++) {
      ListPartsResult.Part part = parts.get(partNumber - 1);
      Assert.assertEquals(partNumber, part.getPartNumber());
      URIStatus status = mFileSystem.getStatus(
          new AlluxioURI(tmpDir + AlluxioURI.SEPARATOR + partNumber));
      Assert.assertEquals(S3RestUtils.toS3Date(status.getLastModificationTimeMs()),
          part.getLastModified());
      Assert.assertEquals(status.getLength(), part.getSize());
    }

    // Call ListParts as a separate FileSystem user
    // Verify 403 HTTP status
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        objectKey, ImmutableMap.of("uploadId", uploadId), HttpMethod.GET,
        getDefaultOptionsWithAuth("dummy")).execute();
    Assert.assertEquals(403, connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.ACCESS_DENIED_ERROR, response.getCode());
  }

  @Test
  public void abortMultipartUpload() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();
    AlluxioURI tmpDir = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        AlluxioURI.SEPARATOR + bucketName, objectName, uploadId));
    Assert.assertTrue(mFileSystem.exists(tmpDir));
    Assert.assertTrue(mFileSystem.getStatus(tmpDir).isFolder());

    HttpURLConnection connection = abortMultipartUploadRestCall(objectKey, uploadId);
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), connection.getResponseCode());
    Assert.assertFalse(mFileSystem.exists(tmpDir));
  }

  @Test
  public void abortMultipartUploadWithNonExistingUploadId() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();
    AlluxioURI tmpDir = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        AlluxioURI.SEPARATOR + bucketName, objectName, uploadId));
    Assert.assertTrue(mFileSystem.exists(tmpDir));
    Assert.assertTrue(mFileSystem.getStatus(tmpDir).isFolder());

    try {
      abortMultipartUploadRestCall(objectKey, uploadId + 1);
    } catch (AssertionError e) {
      // Expected since the upload ID does not exist, the temporary directory should still exist.
      Assert.assertTrue(mFileSystem.exists(tmpDir));
      return;
    }
    Assert.fail("Abort multipart upload with non-existing upload ID should fail");
  }

  @Test
  public void completeMultipartUploadTest() throws Exception {
    int numOfTestIter = 3;
    int maxParts = 50;
    int minParts = 15;
    Random random = new Random();
    for (int i = 0; i < numOfTestIter; i++) {
      completeMultipartUpload(random.nextInt(maxParts - minParts) + minParts);
    }
  }

  public void completeMultipartUpload(int partsNum) throws Exception {
    // Random number of parts in the multipart upload, each part contains a random string,
    // after completion, the object should contain the combination of these parts' content.

    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;

    // Initiate the multipart upload.
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();

    // Upload parts.
    String[] objects = new String[partsNum];
    List<Integer> parts = new ArrayList<>();
    for (int i = 0; i < partsNum; i++) {
      parts.add(i + 1);
    }
    Collections.shuffle(parts);
    for (int partNum : parts) {
      int idx = partNum - 1;
      objects[idx] = CommonUtils.randomAlphaNumString(DATA_SIZE);
      createObject(objectKey, objects[idx].getBytes(), uploadId, partNum);
    }
    // Verify that the two parts are uploaded to the temporary directory.
    AlluxioURI tmpDir = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        AlluxioURI.SEPARATOR + bucketName, objectName, uploadId));
    Assert.assertEquals(partsNum, mFileSystem.listStatus(tmpDir).size());

    // Complete the multipart upload.
    List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
    for (int i = 1; i <= partsNum; i++) {
      partList.add(new CompleteMultipartUploadRequest.Part("", i));
    }
    result = completeMultipartUploadRestCall(objectKey, uploadId,
        new CompleteMultipartUploadRequest(partList));

    // Verify that the response is expected.
    String expectedCombinedObject = String.join("", objects);
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] digest = md5.digest(expectedCombinedObject.getBytes());
    String etag = Hex.encodeHexString(digest);
    String objectPath = AlluxioURI.SEPARATOR + objectKey;
    CompleteMultipartUploadResult completeMultipartUploadResult =
        new CompleteMultipartUploadResult(objectPath, bucketName, objectName, etag);
    Assert.assertEquals(XML_MAPPER.writeValueAsString(completeMultipartUploadResult),
        result.trim());
    Assert.assertEquals(XML_MAPPER.readValue(result, CompleteMultipartUploadResult.class),
        completeMultipartUploadResult);

    // Verify that the temporary directory is deleted.
    Assert.assertFalse(mFileSystem.exists(tmpDir));

    // Verify that the completed object is expected.
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(objectPath))) {
      String combinedObject = IOUtils.toString(is);
      Assert.assertEquals(expectedCombinedObject, combinedObject);
    }
  }

  @Test
  public void duplicateMultipartUpload() throws Exception {
    /*
    1) Test for two mp uploads with diff upload id for creating same object,
    one should overwrite the other
    2) Test for CompleteMultipartUpload call should be idempotent, AKA
    CompleteMultipartUpload made for the same uploadId should return the same result.
     */
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;

    // Initiate the first multipart upload.
    String result1 = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult1 =
        XML_MAPPER.readValue(result1, InitiateMultipartUploadResult.class);
    final String uploadId1 = multipartUploadResult1.getUploadId();

    // Initiate the second multipart upload.
    String result2 = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult2 =
        XML_MAPPER.readValue(result2, InitiateMultipartUploadResult.class);
    final String uploadId2 = multipartUploadResult2.getUploadId();

    // Upload parts for each multipart upload.
    String object1 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    String object2 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObject(objectKey, object1.getBytes(), uploadId1, 1);
    createObject(objectKey, object2.getBytes(), uploadId1, 2);

    String object3 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObject(objectKey, object3.getBytes(), uploadId2, 1);

    // Verify that the parts are uploaded to the corresponding temporary directories.
    AlluxioURI tmpDir1 = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        AlluxioURI.SEPARATOR + bucketName, objectName, uploadId1));
    Assert.assertEquals(2, mFileSystem.listStatus(tmpDir1).size());

    AlluxioURI tmpDir2 = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
        AlluxioURI.SEPARATOR + bucketName, objectName, uploadId2));
    Assert.assertEquals(1, mFileSystem.listStatus(tmpDir2).size());

    // Complete the first multipart upload.
    List<CompleteMultipartUploadRequest.Part> partList1 = new ArrayList<>();
    partList1.add(new CompleteMultipartUploadRequest.Part("", 1));
    partList1.add(new CompleteMultipartUploadRequest.Part("", 2));
    result1 = completeMultipartUploadRestCall(objectKey, uploadId1,
        new CompleteMultipartUploadRequest(partList1));
    String result1Retry = completeMultipartUploadRestCall(objectKey, uploadId1,
            new CompleteMultipartUploadRequest(partList1));

    // Verify that the response is expected.
    String expectedCombinedObject = object1 + object2;
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] digest = md5.digest(expectedCombinedObject.getBytes());
    String etag = Hex.encodeHexString(digest);
    String objectPath = AlluxioURI.SEPARATOR + objectKey;
    CompleteMultipartUploadResult completeMultipartUploadResult1 =
        new CompleteMultipartUploadResult(objectPath, bucketName, objectName, etag);
    Assert.assertEquals(XML_MAPPER.writeValueAsString(completeMultipartUploadResult1),
        result1.trim());
    Assert.assertEquals(XML_MAPPER.readValue(result1, CompleteMultipartUploadResult.class),
        completeMultipartUploadResult1);

    // Verify the response is idempotent for upload1
    Assert.assertEquals(result1, result1Retry);

    // Verify that only the corresponding temporary directory is deleted.
    Assert.assertFalse(mFileSystem.exists(tmpDir1));
    Assert.assertTrue(mFileSystem.exists(tmpDir2));

    // Verify that the completed object is expected.
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(objectPath))) {
      String combinedObject = IOUtils.toString(is);
      Assert.assertEquals(expectedCombinedObject, combinedObject);
    }

    // Complete the second multipart upload.
    List<CompleteMultipartUploadRequest.Part> partList2 = new ArrayList<>();
    partList2.add(new CompleteMultipartUploadRequest.Part("", 1));
    result2 = completeMultipartUploadRestCall(objectKey, uploadId2,
        new CompleteMultipartUploadRequest(partList2));
    String result2Retry = completeMultipartUploadRestCall(objectKey, uploadId2,
            new CompleteMultipartUploadRequest(partList2));

    // Verify the response is idempotent for upload2
    Assert.assertEquals(result2, result2Retry);

    // Verify that the response is expected.
    digest = md5.digest(object3.getBytes());
    etag = Hex.encodeHexString(digest);
    CompleteMultipartUploadResult completeMultipartUploadResult2 =
        new CompleteMultipartUploadResult(objectPath, bucketName, objectName, etag);
    Assert.assertEquals(XML_MAPPER.writeValueAsString(completeMultipartUploadResult2),
        result2.trim());
    Assert.assertEquals(XML_MAPPER.readValue(result2, CompleteMultipartUploadResult.class),
        completeMultipartUploadResult2);

    // Verify that the temporary directory is deleted.
    Assert.assertFalse(mFileSystem.exists(tmpDir2));

    // Verify that the completed object is expected.
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(objectPath))) {
      String newObject = IOUtils.toString(is);
      Assert.assertEquals(object3, newObject);
    }

    // Now if CompleteMultipartUpload is called for upload1
    // It should say NoSuchUpload
    HttpURLConnection connection = completeMultipartUploadRestCallWithResponse(objectKey, uploadId1,
            new CompleteMultipartUploadRequest(partList1));
    Assert.assertEquals(404, connection.getResponseCode());
    S3Error response =
            new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_UPLOAD, response.getCode());
  }

  @Test
  public void completeMultipartUploadWithInvalidArgument() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;

    // Initiate the multipart upload.
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();
    TestCase testCase = getCompleteMultipartUploadReadCallTestCase(objectKey, uploadId, null);
    HttpURLConnection connection = testCase.execute();
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), connection.getResponseCode());
  }

  @Test
  @Ignore
  public void completeMultipartUploadSpecifyParts() throws Exception {
    // This test requires the following property key change
    // Configuration.set(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_MIN_PART_SIZE, "256KB");

    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;

    // Initiate the multipart upload.
    String result = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult =
        XML_MAPPER.readValue(result, InitiateMultipartUploadResult.class);
    final String uploadId = multipartUploadResult.getUploadId();

    // Upload parts.
    String object1 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    String object2 = CommonUtils.randomAlphaNumString(LARGE_DATA_SIZE);
    String object3 = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObject(objectKey, object1.getBytes(), uploadId, 1);
    createObject(objectKey, object2.getBytes(), uploadId, 2);
    createObject(objectKey, object3.getBytes(), uploadId, 3);

    try {
      // Part not found
      List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
      partList.add(new CompleteMultipartUploadRequest.Part("", 1));
      partList.add(new CompleteMultipartUploadRequest.Part("", 2));
      partList.add(new CompleteMultipartUploadRequest.Part("", 3));
      partList.add(new CompleteMultipartUploadRequest.Part("", 4));
      completeMultipartUploadRestCall(objectKey, uploadId,
          new CompleteMultipartUploadRequest(partList, true));
    } catch (AssertionError e) {
      // expected
    }

    try {
      // Invalid part order
      List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
      partList.add(new CompleteMultipartUploadRequest.Part("", 2));
      partList.add(new CompleteMultipartUploadRequest.Part("", 1));
      partList.add(new CompleteMultipartUploadRequest.Part("", 3));
      completeMultipartUploadRestCall(objectKey, uploadId,
          new CompleteMultipartUploadRequest(partList, true));
    } catch (AssertionError e) {
      // expected
    }
    try {
      // Parts are too small
      List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
      partList.add(new CompleteMultipartUploadRequest.Part("", 1));
      partList.add(new CompleteMultipartUploadRequest.Part("", 2));
      partList.add(new CompleteMultipartUploadRequest.Part("", 3));
      completeMultipartUploadRestCall(objectKey, uploadId,
          new CompleteMultipartUploadRequest(partList, true));
    } catch (AssertionError e) {
      // expected
    }

    // Complete using a partial list of available parts
    // - Part 2 satisfies size requirements, part 3 is not subject to the requirement
    List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
    partList.add(new CompleteMultipartUploadRequest.Part("", 2));
    partList.add(new CompleteMultipartUploadRequest.Part("", 3));
    completeMultipartUploadRestCall(objectKey, uploadId,
        new CompleteMultipartUploadRequest(partList, true));
  }

  @Test
  public void listMultipartUploads() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;

    // Initiate the first multipart upload.
    String result1 = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult1 =
        XML_MAPPER.readValue(result1, InitiateMultipartUploadResult.class);
    final String uploadId1 = multipartUploadResult1.getUploadId();

    // Initiate the second multipart upload.
    String result2 = initiateMultipartUploadRestCall(objectKey);
    InitiateMultipartUploadResult multipartUploadResult2 =
        XML_MAPPER.readValue(result2, InitiateMultipartUploadResult.class);
    final String uploadId2 = multipartUploadResult2.getUploadId();

    // Create a multipart upload for a different bucket
    final String otherBucketName = "other_bucket";
    createBucketRestCall(otherBucketName);

    String otherObjectKey = otherBucketName + AlluxioURI.SEPARATOR + objectName;
    String otherResult = initiateMultipartUploadRestCall(otherObjectKey);
    InitiateMultipartUploadResult otherMultipartUploadResult =
        XML_MAPPER.readValue(otherResult, InitiateMultipartUploadResult.class);
    final String otherUploadId = otherMultipartUploadResult.getUploadId();

    // Fetch multipart uploads for the first bucket
    String result = listMultipartUploadsRestCall(bucketName);
    ListMultipartUploadsResult listUploadsResult = XML_MAPPER.readValue(
        result, ListMultipartUploadsResult.class);
    Map<String, String> uploads = new HashMap<>();
    for (ListMultipartUploadsResult.Upload upload : listUploadsResult.getUploads()) {
      uploads.put(upload.getUploadId(), upload.getKey());
    }
    assertEquals(2, uploads.size());
    Assert.assertEquals(objectName, uploads.get(uploadId1));
    Assert.assertEquals(objectName, uploads.get(uploadId2));

    // Fetch multipart uploads for the second bucket
    result = listMultipartUploadsRestCall(otherBucketName);
    listUploadsResult = XML_MAPPER.readValue(result, ListMultipartUploadsResult.class);
    uploads.clear();
    for (ListMultipartUploadsResult.Upload upload : listUploadsResult.getUploads()) {
      uploads.put(upload.getUploadId(), upload.getKey());
    }
    assertEquals(1, uploads.size());
    Assert.assertEquals(objectName, uploads.get(otherUploadId));

    // Call ListMultipartUploads as a separate FileSystem user
    // Verify that they do not see these uploads
    result = listMultipartUploadsRestCall(bucketName, "dummy");
    listUploadsResult = XML_MAPPER.readValue(result, ListMultipartUploadsResult.class);
    assertNull(listUploadsResult.getUploads());

    result = listMultipartUploadsRestCall(otherBucketName, "dummy");
    listUploadsResult = XML_MAPPER.readValue(result, ListMultipartUploadsResult.class);
    assertNull(listUploadsResult.getUploads());

    // Abort a multipart upload
    abortMultipartUploadRestCall(objectKey, uploadId1);
    result = listMultipartUploadsRestCall(bucketName);
    listUploadsResult = XML_MAPPER.readValue(result, ListMultipartUploadsResult.class);
    uploads.clear();
    for (ListMultipartUploadsResult.Upload upload : listUploadsResult.getUploads()) {
      uploads.put(upload.getUploadId(), upload.getKey());
    }
    assertEquals(1, uploads.size());
    Assert.assertFalse(uploads.containsKey(uploadId1));
    Assert.assertEquals(objectName, uploads.get(uploadId2));

    // Complete a multipart upload
    String object = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObject(objectKey, object.getBytes(), uploadId2, 1); // Upload a part
    List<CompleteMultipartUploadRequest.Part> partList = new ArrayList<>();
    partList.add(new CompleteMultipartUploadRequest.Part("", 1));
    completeMultipartUploadRestCall(objectKey, uploadId2,
        new CompleteMultipartUploadRequest(partList));
    result = listMultipartUploadsRestCall(bucketName);
    listUploadsResult = XML_MAPPER.readValue(result, ListMultipartUploadsResult.class);
    assertNull(listUploadsResult.getUploads());
  }

  @Test
  public void testObjectContentType() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String objectData = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData)
            .setContentType(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes())));

    HttpURLConnection connection = getObjectMetadataRestCall(objectKey);
    Assert.assertEquals(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE,
        connection.getHeaderField(TestCaseOptions.CONTENT_TYPE_HEADER));
  }

  @Test
  public void testBucketTagging() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);
    testTagging(bucketName, ImmutableMap.of());
  }

  @Test
  public void testObjectTagsHeader() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String objectData = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData.getBytes())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes()))
            .addHeader(S3Constants.S3_TAGGING_HEADER, "foo=bar&baz"));

    testTagging(objectKey, ImmutableMap.of(
        "foo", "bar",
        "baz", ""
    ));
  }

  @Test
  @Ignore
  public void testTaggingNoLimit() throws Exception {
    // This test requires the following property key change
    // Configuration.set(PropertyKey.PROXY_S3_TAGGING_RESTRICTIONS_ENABLED, false);

    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String objectData = CommonUtils.randomAlphaNumString(DATA_SIZE);

    String longTagKey = Strings.repeat("a", 128 + 1);
    String longTagValue = Strings.repeat("b", 256 + 1);
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData.getBytes())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes()))
            .addHeader(S3Constants.S3_TAGGING_HEADER, String.format(
                "tag1&tag2&tag3&tag4&tag5&tag6&tag7&tag8&tag9&tag10&%s=%s",
                longTagKey, longTagValue)));

    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("tag1", "");
    tagMap.put("tag2", "");
    tagMap.put("tag3", "");
    tagMap.put("tag4", "");
    tagMap.put("tag5", "");
    tagMap.put("tag6", "");
    tagMap.put("tag7", "");
    tagMap.put("tag8", "");
    tagMap.put("tag9", "");
    tagMap.put("tag10", "");
    tagMap.put(longTagKey, longTagValue);
    testTagging(objectKey, ImmutableMap.copyOf(tagMap));
  }

  /**
   * the test case is that when you copy an object from one folder to a different folder,
   * the parent directories of this target path will be created.
   * @throws Exception
   */
  @Test
  public void testCopyObject() throws Exception {
    final String bucketName = "bucket";
    final String objectKey = "object";
    final String targetObject = "/nonExistDir/copyTarget";

    String object = CommonUtils.randomAlphaNumString(DATA_SIZE);
    final String fullObjectKey = bucketName + AlluxioURI.SEPARATOR + objectKey;
    String copiedObjectKey = bucketName + targetObject;
    AlluxioURI copiedObjectURI = new AlluxioURI(AlluxioURI.SEPARATOR + copiedObjectKey);

    createBucketRestCall(bucketName);
    createObject(fullObjectKey, object.getBytes(), null, null);

    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        copiedObjectKey,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, fullObjectKey)).runAndGetResponse();

    List<FileInfo> fileInfos =
        mFileSystemMaster.listStatus(copiedObjectURI, ListStatusContext.defaults());
    Assert.assertEquals(1, fileInfos.size());
    Assert.assertEquals(copiedObjectURI.getPath(), fileInfos.get(0).getPath());

    // Verify the object's content.
    FileInStream is = mFileSystem.openFile(copiedObjectURI);
    byte[] writtenObjectContent = IOUtils.toString(is).getBytes();
    is.close();
    Assert.assertArrayEquals(object.getBytes(), writtenObjectContent);
    Assert.assertNotNull(fileInfos.get(0).getXAttr());
    Assert.assertEquals(
        Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(writtenObjectContent)),
        new String(fileInfos.get(0).getXAttr().get(S3Constants.ETAG_XATTR_KEY),
            S3Constants.XATTR_STR_CHARSET));
  }

  @Test
  public void testCopyObjectMetadata() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String objectData = "text data";
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData)
            .setContentType(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes()))
            .addHeader(S3Constants.S3_TAGGING_HEADER, "foo=bar&baz"));

    TaggingData newTags = getTagsRestCall(objectKey);
    Assert.assertEquals(ImmutableMap.of(
        "foo", "bar",
        "baz", ""
    ), newTags.getTagMap());

    // metadata directive = COPY, tagging directive = COPY
    String copiedObjectKey = String.format("%s%s%s", bucketName, AlluxioURI.SEPARATOR,
        "copyMeta_copyTags_object");
    new TestCase(mHostname, mPort, mBaseUri,
        copiedObjectKey,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, objectKey)).runAndGetResponse();
    newTags = getTagsRestCall(copiedObjectKey);
    Assert.assertEquals(ImmutableMap.of(
        "foo", "bar",
        "baz", ""
    ), newTags.getTagMap());
    HttpURLConnection connection = getObjectMetadataRestCall(copiedObjectKey);
    Assert.assertEquals(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE, connection.getContentType());
    assertEquals(objectData, getObjectRestCall(copiedObjectKey));

    // metadata directive = REPLACE, tagging directive = COPY
    copiedObjectKey = String.format("%s%s%s", bucketName, AlluxioURI.SEPARATOR,
        "replaceMeta_copyTags_object");
    new TestCase(mHostname, mPort, mBaseUri,
        copiedObjectKey,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, objectKey)
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE))
        .runAndGetResponse();
    newTags = getTagsRestCall(copiedObjectKey);
    Assert.assertEquals(ImmutableMap.of(
        "foo", "bar",
        "baz", ""
    ), newTags.getTagMap());
    connection = getObjectMetadataRestCall(copiedObjectKey);
    Assert.assertEquals(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE, connection.getContentType());
    assertEquals(objectData, getObjectRestCall(copiedObjectKey));

    // metadata directive = COPY, tagging directive = REPLACE
    copiedObjectKey = String.format("%s%s%s", bucketName, AlluxioURI.SEPARATOR,
        "copyMeta_replaceTags_object");
    new TestCase(mHostname, mPort, mBaseUri,
        copiedObjectKey,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, objectKey)
            .addHeader(S3Constants.S3_TAGGING_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_TAGGING_HEADER, "foo=new"))
        .runAndGetResponse();
    newTags = getTagsRestCall(copiedObjectKey);
    Assert.assertEquals(ImmutableMap.of(
        "foo", "new"
    ), newTags.getTagMap());
    connection = getObjectMetadataRestCall(copiedObjectKey);
    Assert.assertEquals(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE, connection.getContentType());
    assertEquals(objectData, getObjectRestCall(copiedObjectKey));

    // metadata directive = REPLACE, tagging directive = REPLACE
    copiedObjectKey = String.format("%s%s%s", bucketName, AlluxioURI.SEPARATOR,
        "replaceMeta_replaceTags_object");
    new TestCase(mHostname, mPort, mBaseUri,
        copiedObjectKey,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, objectKey)
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .addHeader(S3Constants.S3_TAGGING_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_TAGGING_HEADER, "foo=new"))
        .runAndGetResponse();
    newTags = getTagsRestCall(copiedObjectKey);
    Assert.assertEquals(ImmutableMap.of(
        "foo", "new"
    ), newTags.getTagMap());
    connection = getObjectMetadataRestCall(copiedObjectKey);
    Assert.assertEquals(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE, connection.getContentType());
    assertEquals(objectData, getObjectRestCall(copiedObjectKey));
  }

  @Test
  public void testObjectTagging() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String objectName = "object";
    String objectKey = bucketName + AlluxioURI.SEPARATOR + objectName;
    String objectData = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData.getBytes())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes())));

    testTagging(objectKey, ImmutableMap.of());
  }

  @Test
  public void testFolderTagging() throws Exception {
    final String bucketName = "bucket";
    createBucketRestCall(bucketName);

    final String folderName = "folder";
    String folderKey = bucketName + AlluxioURI.SEPARATOR + folderName;
    final String objectName = "object";
    String objectKey = folderKey + AlluxioURI.SEPARATOR + objectName;
    String objectData = CommonUtils.randomAlphaNumString(DATA_SIZE);
    createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(objectData.getBytes())
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(objectData.getBytes()))
            .addHeader(S3Constants.S3_TAGGING_HEADER, "foo=bar"));

    // Ensure that folders are not populated with tags from children
    testTagging(folderKey, ImmutableMap.of());
  }

  private void testTagging(String resource, ImmutableMap<String, String> expectedTags)
      throws Exception {
    // Get{...}Tagging
    TaggingData tagData = getTagsRestCall(resource);
    if (expectedTags != null) { // allow skipping checking of initial tags
      Assert.assertEquals(expectedTags, tagData.getTagMap());
    }

    // Put{...}Tagging
    Map<String, String> tagMap = ImmutableMap.of(
        "foo", "bar",
        "fu", "bar",
        "baz", ""
    );
    tagData.clear();
    tagData.addTags(tagMap);
    putTagsRestCall(resource, tagData);

    // Get{...}Tagging
    TaggingData newTags = getTagsRestCall(resource);
    Assert.assertEquals(tagMap, newTags.getTagMap());

    // Delete{...}Tagging
    deleteTagsRestCall(resource);
    TaggingData deletedTags = getTagsRestCall(resource);
    Assert.assertEquals(0, deletedTags.getTagMap().size());
  }

  private HttpURLConnection deleteBucketRestCall(String bucketUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        bucketUri, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).executeAndAssertSuccess();
  }

  private String computeObjectChecksum(byte[] objectContent) throws Exception {
    MessageDigest md5Hash = MessageDigest.getInstance("MD5");
    byte[] md5Digest = md5Hash.digest(objectContent);
    return BaseEncoding.base64().encode(md5Digest);
  }

  private void createObjectRestCall(String objectUri, @NotNull Map<String, String> params,
                                    @NotNull TestCaseOptions options) throws Exception {
    new TestCase(mHostname, mPort, mBaseUri, objectUri, params, HttpMethod.PUT, options)
        .runAndCheckResult();
  }

  private String initiateMultipartUploadRestCall(String objectUri) throws Exception {
    return initiateMultipartUploadRestCall(objectUri, TEST_USER_NAME);
  }

  private String initiateMultipartUploadRestCall(String objectUri, String user) throws Exception {
    TestCaseOptions options = getDefaultOptionsWithAuth(user);
    Map<String, String> params = ImmutableMap.of("uploads", "");
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, params, HttpMethod.POST,
        options).runAndGetResponse();
  }

  private TestCase getCompleteMultipartUploadReadCallTestCase(
          String objectUri, String uploadId, CompleteMultipartUploadRequest request) {
    Map<String, String> params = ImmutableMap.of("uploadId", uploadId);
    return new TestCase(mHostname, mPort, mBaseUri,
            objectUri, params, HttpMethod.POST,
            getDefaultOptionsWithAuth()
                    .setBody(request)
                    .setContentType(TestCaseOptions.XML_CONTENT_TYPE));
  }

  private String completeMultipartUploadRestCall(
          String objectUri, String uploadId, CompleteMultipartUploadRequest request)
          throws Exception {
    TestCase testCase = getCompleteMultipartUploadReadCallTestCase(objectUri, uploadId, request);
    return testCase.runAndGetResponse();
  }

  private HttpURLConnection completeMultipartUploadRestCallWithResponse(
          String objectUri, String uploadId, CompleteMultipartUploadRequest request)
          throws Exception {
    TestCase testCase = getCompleteMultipartUploadReadCallTestCase(objectUri, uploadId, request);
    return testCase.execute();
  }

  private HttpURLConnection abortMultipartUploadRestCall(String objectUri, String uploadId)
      throws Exception {
    Map<String, String> params = ImmutableMap.of("uploadId", uploadId);
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, params, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).executeAndAssertSuccess();
  }

  private String listPartsRestCall(String objectUri, String uploadId)
      throws Exception {
    Map<String, String> params = ImmutableMap.of("uploadId", uploadId);
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, params, HttpMethod.GET,
        getDefaultOptionsWithAuth()).runAndGetResponse();
  }

  private String listMultipartUploadsRestCall(String bucketUri) throws Exception {
    return listMultipartUploadsRestCall(bucketUri, TEST_USER_NAME);
  }

  private String listMultipartUploadsRestCall(String bucketUri, String user) throws Exception {
    TestCaseOptions options = getDefaultOptionsWithAuth(user);
    return new TestCase(mHostname, mPort, mBaseUri,
        bucketUri, ImmutableMap.of("uploads", ""), HttpMethod.GET,
        options).runAndGetResponse();
  }

  private HttpURLConnection getObjectMetadataRestCall(String objectUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.HEAD,
        getDefaultOptionsWithAuth()).executeAndAssertSuccess();
  }

  private String getObjectRestCall(String objectUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth()).runAndGetResponse();
  }

  /**
   * Do not process the error response, and judge by the method caller.
   * @param objectUri object access uri
   * @return connection
   * @throws Exception
   */
  private HttpURLConnection getObjectRestCallWithError(String objectUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth()).execute();
  }

  private void deleteObjectRestCall(String objectUri) throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).runAndCheckResult();
  }

  private void deleteTagsRestCall(String uri) throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        uri, ImmutableMap.of("tagging", ""), HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).runAndCheckResult();
  }

  private TaggingData getTagsRestCall(String uri) throws Exception {
    String res = new TestCase(mHostname, mPort, mBaseUri,
        uri, ImmutableMap.of("tagging", ""), HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE)
    ).runAndGetResponse();
    XmlMapper mapper = new XmlMapper();
    return mapper.readValue(res, TaggingData.class);
  }

  private void putTagsRestCall(String uri, @NotNull TaggingData tags) throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        uri, ImmutableMap.of("tagging", ""), HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setCharset(S3Constants.TAGGING_CHARSET)
            .setBody(TaggingData.serialize(tags).toByteArray()))
        .runAndCheckResult();
  }

  @Test
  public void testMalformedAuthHeader() throws Exception {
    // test with Null Authorization Header
    String bucket = "test-bucket";
    TestCaseOptions options = getDefaultOptionsWithAuth();
    options.setAuthorization("");
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
            bucket, NO_PARAMS, HttpMethod.GET, options).execute();
    Assert.assertEquals(400, connection.getResponseCode());
    S3Error response =
            new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.AUTHORIZATION_HEADER_MALFORMED, response.getCode());

    // test with V2 Authorization Header
    options = getDefaultOptionsWithAuth();
    options.setAuthorization("AWS alluxio:3uRmVm7lWfvclsqfpPJN2Ftigi4=");
    connection = new TestCase(mHostname, mPort, mBaseUri,
            bucket, NO_PARAMS, HttpMethod.GET, options).execute();
    Assert.assertEquals(400, connection.getResponseCode());
    response =
            new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(S3ErrorCode.Name.AUTHORIZATION_HEADER_MALFORMED, response.getCode());
  }
}
