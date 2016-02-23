/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.thrift.CompleteFileTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The method option for completing a file.
 */
@PublicApi
@NotThreadSafe
public final class CompleteFileOptions {
  private long mUfsLength;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new CompleteFileOptions();
  }

  private CompleteFileOptions() {
    mUfsLength = 0;
  }

  /**
   * @return the UFS file length
   */
  public long getUfsLength() {
    return mUfsLength;
  }

  /**
   * @param ufsLength the UFS file length to use
   * @return the updated options object
   */
  public CompleteFileOptions setUfsLength(long ufsLength) {
    mUfsLength = ufsLength;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CompleteFileOptions(");
    sb.append(super.toString()).append(", UFS Length: ").append(mUfsLength);
    sb.append(")");
    return sb.toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CompleteFileTOptions toThrift() {
    CompleteFileTOptions options = new CompleteFileTOptions();
    options.setUfsLength(mUfsLength);
    return options;
  }
}
