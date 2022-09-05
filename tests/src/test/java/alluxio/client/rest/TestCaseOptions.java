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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;

/**
 * Method options for creating a REST API test case.
 */
// TODO(jiri): consolidate input stream and body fields
@NotThreadSafe
public final class TestCaseOptions {
  /* Supported content types */
  public static final String JSON_CONTENT_TYPE = MediaType.APPLICATION_JSON;
  public static final String OCTET_STREAM_CONTENT_TYPE = MediaType.APPLICATION_OCTET_STREAM;
  public static final String XML_CONTENT_TYPE = MediaType.APPLICATION_XML;
  public static final String TEXT_PLAIN_CONTENT_TYPE = MediaType.TEXT_PLAIN;

  /* Headers */
  public static final String AUTHORIZATION_HEADER = "Authorization";
  public static final String CONTENT_TYPE_HEADER = "Content-Type";
  public static final String CONTENT_MD5_HEADER = "Content-MD5";
  private String mAuthorization;
  private String mContentType;
  private String mMD5;

  // mHeaders contains the previously defined headers
  // - Users may add additional headers
  private final Map<String, String> mHeaders;

  private Object mBody;
  private Charset mCharset; // used when converting byte data into strings
  private boolean mPrettyPrint; // used for ObjectMapper when printing strings

  /**
   * @return the default {@link TestCaseOptions}
   */
  public static TestCaseOptions defaults() {
    TestCaseOptions options =  new TestCaseOptions();
    options.setAuthorization("AWS4-HMAC-SHA256 Credential=alluxio/20220830");
    return options;
  }

  private TestCaseOptions() {
    mAuthorization = null;
    mBody = null;
    mContentType = OCTET_STREAM_CONTENT_TYPE;
    mCharset = StandardCharsets.UTF_8;
    mHeaders = new HashMap<>();
    mMD5 = null;
    mPrettyPrint = false;
  }

  /**
   * @return the object representing the data to be sent to the web server
   */
  public Object getBody() {
    return mBody;
  }

  /**
   * @return the pretty print flag
   */
  public boolean isPrettyPrint() {
    return mPrettyPrint;
  }

  /**
   * @return the content type
   */
  public String getContentType() {
    return mContentType;
  }

  /**
   * @return the Base64 encoded MD5 digest of the request body
   */
  public String getMD5() {
    return mMD5;
  }

  /**
   * @return the authorization header
   */
  public String getAuthorization() {
    return mAuthorization;
  }

  /**
   * @return the charset map
   */
  public Charset getCharset() {
    return mCharset;
  }

  /**
   * @return the headers map
   */
  public Map<String, String> getHeaders() {
    return mHeaders;
  }

  /**
   * @param body the body to use
   * @return the updated options object
   */
  public TestCaseOptions setBody(Object body) {
    mBody = body;
    return this;
  }

  /**
   * @param prettyPrint the pretty print flag value to use
   * @return the updated options object
   */
  public TestCaseOptions setPrettyPrint(boolean prettyPrint) {
    mPrettyPrint = prettyPrint;
    return this;
  }

  /**
   * @param contentType the content type to set
   * @return the updated options object
   */
  public TestCaseOptions setContentType(String contentType) {
    mContentType = contentType;
    mHeaders.put(CONTENT_TYPE_HEADER, contentType);
    return this;
  }

  /**
   * @param md5 the Base64 encoded MD5 digest of the request body
   * @return the updated options object
   */
  public TestCaseOptions setMD5(String md5) {
    mMD5 = md5;
    mHeaders.put(CONTENT_MD5_HEADER, md5);
    return this;
  }

  /**
   * @param authorization the authorization header
   * @return the updated options object
   */
  public TestCaseOptions setAuthorization(String authorization) {
    mAuthorization = authorization;
    mHeaders.put(AUTHORIZATION_HEADER, authorization);
    return this;
  }

  /**
   * @param charset the charset to use
   * @return the updated options object
   */
  public TestCaseOptions setCharset(@NotNull Charset charset) {
    mCharset = charset;
    return this;
  }

  /**
   * Adds the provided headers to the existing headers map. Overwrites duplicate keys.
   * Note that this does not update the class header fields if you overwrite them.
   * @param headers headers map
   * @return the updated options object
   */
  public TestCaseOptions addHeaders(@NotNull Map<String, String> headers) {
    mHeaders.putAll(headers);
    return this;
  }

  /**
   * Adds the provided header to the existing headers map. Overwrites duplicate keys.
   * Note that this does not update the class header fields if you overwrite them.
   * @param key header key
   * @param value header value
   * @return the updated options object
   */
  public TestCaseOptions addHeader(String key, String value) {
    mHeaders.put(key, value);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TestCaseOptions)) {
      return false;
    }
    TestCaseOptions that = (TestCaseOptions) o;
    return Objects.equal(mAuthorization, that.mAuthorization)
        && Objects.equal(mBody, that.mBody)
        && Objects.equal(mCharset, that.mCharset)
        && Objects.equal(mContentType, that.mContentType)
        && Objects.equal(mHeaders, that.mHeaders)
        && Objects.equal(mMD5, that.mMD5)
        && mPrettyPrint == that.mPrettyPrint;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAuthorization, mBody, mCharset, mContentType, mHeaders, mMD5,
        mPrettyPrint);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authorization", mAuthorization)
        .add("body", mBody)
        .add("charset", mCharset)
        .add("content type", mContentType)
        .add("headers", mHeaders)
        .add("MD5", mMD5)
        .add("pretty print", mPrettyPrint)
        .toString();
  }
}
