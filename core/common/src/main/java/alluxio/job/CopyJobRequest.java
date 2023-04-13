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

package alluxio.job;

import alluxio.grpc.CopyJobPOptions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The request of loading files.
 */
@ThreadSafe
public class CopyJobRequest implements JobRequest {
  private static final String TYPE = "copy";
  private static final long serialVersionUID = -8565405317284410500L;
  private final String mDst;
  private final CopyJobPOptions mOptions;
  private final String mSrc;

  /**
   * @param src the source file path
   * @param dst the destination file path
   * @param options copy job options
   **/
  public CopyJobRequest(@JsonProperty("src") String src,
      @JsonProperty("dst") String dst,
      @JsonProperty("copyJobPOptions") CopyJobPOptions options) {
    mSrc = Preconditions.checkNotNull(src, "The source path cannot be null");

    mDst = Preconditions.checkNotNull(dst, "The destination path cannot be null");
    mOptions = Preconditions.checkNotNull(options, "The job options cannot be null");
  }

  /**
   * @return the source file path
   */
  public String getSrc() {
    return mSrc;
  }

    /**
   * @return the file path
   */
  public String getDst() {
    return mDst;
  }

  /**
   * @return job options
   */
  public CopyJobPOptions getOptions() {
    return mOptions;
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(this)
        .add("Src", mSrc)
        .add("Dst", mDst)
        .add("Options", mOptions)
        .toString();
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
