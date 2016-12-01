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

package alluxio.rest;

import com.google.common.base.Objects;

import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a REST API test case.
 */
// TODO(jiri): consolidate input stream and body fields
@NotThreadSafe
public final class TestCaseOptions {
  private Object mBody;
  private InputStream mInputStream;
  private boolean mPrettyPrint;

  /**
   * @return the default {@link TestCaseOptions}
   */
  public static TestCaseOptions defaults() {
    return new TestCaseOptions();
  }

  private TestCaseOptions() {
    mBody = null;
    mInputStream = null;
    mPrettyPrint = false;
  }

  /**
   * @return the object representing the data to be sent to the web server
   */
  public Object getBody() {
    return mBody;
  }

  /**
   * @return the input stream representing data to be sent to the web server
   */
  public InputStream getInputStream() {
    return mInputStream;
  }

  /**
   * @return the pretty print flag
   */
  public boolean isPrettyPrint() {
    return mPrettyPrint;
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
   * @param inputStream the input stream to use
   * @return the updated options object
   */
  public TestCaseOptions setInputStream(InputStream inputStream) {
    mInputStream = inputStream;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TestCaseOptions)) {
      return false;
    }
    TestCaseOptions that = (TestCaseOptions) o;
    return Objects.equal(mBody, that.mBody)
        && Objects.equal(mInputStream, that.mInputStream)
        && mPrettyPrint == that.mPrettyPrint;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBody, mInputStream, mPrettyPrint);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("body", mBody)
        .add("input stream", mInputStream)
        .add("pretty print", mPrettyPrint)
        .toString();
  }
}
