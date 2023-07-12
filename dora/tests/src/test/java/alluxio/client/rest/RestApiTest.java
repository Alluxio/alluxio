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
import alluxio.proxy.s3.ListBucketResult;
import alluxio.testutils.BaseIntegrationTest;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import java.net.HttpURLConnection;
import java.security.MessageDigest;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.HttpMethod;

public abstract class RestApiTest extends BaseIntegrationTest {
  protected static final Map<String, String> NO_PARAMS = ImmutableMap.of();
  protected static final String TEST_USER_NAME = "testuser";
  protected static final String TEST_BUCKET_NAME = "bucket";
  protected String mHostname;
  protected int mPort;
  protected String mBaseUri = Constants.REST_API_PREFIX;

  protected HttpURLConnection createBucketRestCall(String bucket, @NotNull String user)
      throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        bucket, NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth(user)).execute();
  }

  protected HttpURLConnection createBucketRestCall(String bucket) throws Exception {
    return createBucketRestCall(bucket, TEST_USER_NAME);
  }

  protected HttpURLConnection headRestCall(String uri, @NotNull String user)
      throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        uri, NO_PARAMS, HttpMethod.HEAD,
        getDefaultOptionsWithAuth(user)).execute();
  }

  protected HttpURLConnection headRestCall(String uri) throws Exception {
    return headRestCall(uri, TEST_USER_NAME);
  }

  protected void listStatusRestCall(Map<String, String> parameters, ListBucketResult expected)
      throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        TEST_BUCKET_NAME, parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth())
        .runAndCheckResult(expected);
  }

  protected HttpURLConnection createObjectRestCall(String objectUri,
                                                   @NotNull Map<String, String> params,
                                                   @NotNull TestCaseOptions options)
      throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri, objectUri, params, HttpMethod.PUT, options)
        .execute();
  }

  protected HttpURLConnection createObjectRestCall(String objectUri,
                                                   @NotNull Map<String, String> params,
                                                   byte[] object, @NotNull String user)
      throws Exception {
    return createObjectRestCall(objectUri, params,
        getDefaultOptionsWithAuth(user)
            .setBody(object)
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(computeObjectChecksum(object)));
  }

  protected String getObjectRestCall(String objectUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth()).runAndGetResponse();
  }


  protected HttpURLConnection deleteRestCall(String objectUri) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri,
        objectUri, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).executeAndAssertSuccess();
  }

  protected TestCaseOptions getDefaultOptionsWithAuth(@NotNull String user) {
    return TestCaseOptions.defaults()
        .setAuthorization("AWS4-HMAC-SHA256 Credential=" + user + "/...")
        .setContentType(TestCaseOptions.XML_CONTENT_TYPE);
  }

  protected TestCaseOptions getDefaultOptionsWithAuth() {
    return getDefaultOptionsWithAuth(TEST_USER_NAME);
  }

  private String computeObjectChecksum(byte[] objectContent) throws Exception {
    MessageDigest md5Hash = MessageDigest.getInstance("MD5");
    byte[] md5Digest = md5Hash.digest(objectContent);
    return BaseEncoding.base64().encode(md5Digest);
  }
}
