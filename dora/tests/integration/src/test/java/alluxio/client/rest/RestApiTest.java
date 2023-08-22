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

import alluxio.Constants;
import alluxio.proxy.s3.CompleteMultipartUploadRequest;
import alluxio.s3.S3Constants;
import alluxio.testutils.BaseIntegrationTest;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.HttpMethod;

public abstract class RestApiTest extends BaseIntegrationTest {
  protected static final Map<String, String> NO_PARAMS = ImmutableMap.of();
  protected static final byte[] EMPTY_CONTENT = new byte[] {};
  protected static final String TEST_USER = "testuser";
  protected static final String TEST_BUCKET = "test-bucket";
  protected String mHostname;
  protected int mPort;
  protected String mBaseUri = Constants.REST_API_PREFIX;

  protected TestCase runTestCase(String bucket, Map<String, String> params,
                                 String httpMethod, TestCaseOptions options) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri, bucket, params, httpMethod, options).run();
  }

  protected TestCase createBucketTestCase(String bucket) throws Exception {
    return runTestCase(bucket, NO_PARAMS, HttpMethod.PUT, getDefaultOptionsWithAuth());
  }

  protected TestCase createObjectTestCase(String uri, byte[] object) throws Exception {
    return runTestCase(uri, NO_PARAMS, HttpMethod.PUT, getDefaultOptionsWithAuth()
        .setBody(object)
        .setMD5(computeObjectChecksum(object)));
  }

  protected TestCase createObjectTestCase(String uri, TestCaseOptions options)
      throws Exception {
    return runTestCase(uri, NO_PARAMS, HttpMethod.PUT, options);
  }

  protected TestCase copyObjectTestCase(String sourceUri, String targetUri) throws Exception {
    return runTestCase(targetUri, NO_PARAMS, HttpMethod.PUT, getDefaultOptionsWithAuth()
        .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
            S3Constants.Directive.REPLACE.name())
        .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourceUri));
  }

  protected TestCase deleteTestCase(String uri) throws Exception {
    return runTestCase(uri, NO_PARAMS, HttpMethod.DELETE, getDefaultOptionsWithAuth());
  }

  protected TestCase headTestCase(String uri) throws Exception {
    return runTestCase(uri, NO_PARAMS, HttpMethod.HEAD, getDefaultOptionsWithAuth());
  }

  protected TestCase getTestCase(String uri) throws Exception {
    return runTestCase(uri, NO_PARAMS, HttpMethod.GET, getDefaultOptionsWithAuth());
  }

  protected TestCase listTestCase(String uri, Map<String, String> params) throws Exception {
    return runTestCase(uri, params, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE));
  }

  protected TestCase initiateMultipartUploadTestCase(String uri) throws Exception {
    return runTestCase(
        uri, ImmutableMap.of("uploads", ""), HttpMethod.POST,
        getDefaultOptionsWithAuth());
  }

  protected TestCase completeMultipartUploadTestCase(
      String objectUri, String uploadId, CompleteMultipartUploadRequest request) throws Exception {
    return runTestCase(
        objectUri, ImmutableMap.of("uploadId", uploadId), HttpMethod.POST,
        getDefaultOptionsWithAuth()
            .setBody(request)
            .setContentType(TestCaseOptions.XML_CONTENT_TYPE));
  }

  protected TestCase abortMultipartUploadTestCase(String uri, String uploadId) throws Exception {
    return runTestCase(
        uri, ImmutableMap.of("uploadId", uploadId), HttpMethod.DELETE,
        getDefaultOptionsWithAuth());
  }

  protected TestCase uploadPartTestCase(String uri, byte[] object, String uploadId,
                                        Integer partNumber) throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("uploadId", uploadId);
    params.put("partNumber", partNumber.toString());
    return runTestCase(uri, params, HttpMethod.PUT, getDefaultOptionsWithAuth()
        .setBody(object)
        .setMD5(computeObjectChecksum(object)));
  }

  protected TestCaseOptions getDefaultOptionsWithAuth(@NotNull String user) {
    return TestCaseOptions.defaults()
        .setAuthorization("AWS4-HMAC-SHA256 Credential=" + user + "/...");
  }

  protected TestCaseOptions getDefaultOptionsWithAuth() {
    return getDefaultOptionsWithAuth(TEST_USER);
  }

  private String computeObjectChecksum(byte[] objectContent) throws Exception {
    MessageDigest md5Hash = MessageDigest.getInstance("MD5");
    byte[] md5Digest = md5Hash.digest(objectContent);
    return BaseEncoding.base64().encode(md5Digest);
  }
}
