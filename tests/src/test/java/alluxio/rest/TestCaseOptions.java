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
@NotThreadSafe
public final class TestCaseOptions {
  private InputStream mInputStream;
  private String mJsonString;
  private boolean mPrettyPrint;

  /**
   * @return the default {@link TestCaseOptions}
   */
  public static TestCaseOptions defaults() {
    return new TestCaseOptions();
  }

  private TestCaseOptions() {
    mInputStream = null;
    mJsonString = null;
    mPrettyPrint = false;
  }

  /**
   * @return the input stream
   */
  public InputStream getInputStream() {
    return mInputStream;
  }

  /**
   * @return the JSON string
   */
  public String getJsonString() {
    return mJsonString;
  }

  /**
   * @return the pretty print flag
   */
  public boolean getPrettyPrint() {
    return mPrettyPrint;
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
   * @param jsonString the JSON string to use
   * @return the updated options object
   */
  public TestCaseOptions setJsonString(String jsonString) {
    mJsonString = jsonString;
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
    return Objects.equal(mInputStream, that.mInputStream)
        && Objects.equal(mJsonString, that.mJsonString)
        && mPrettyPrint == that.mPrettyPrint;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInputStream, mJsonString, mPrettyPrint);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("input stream", mInputStream)
        .add("JSON string", mJsonString)
        .add("pretty print", mPrettyPrint)
        .toString();
  }
}
