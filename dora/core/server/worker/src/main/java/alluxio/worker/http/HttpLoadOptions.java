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

package alluxio.worker.http;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * A data model used for describing the HTTP load options.
 */
public class HttpLoadOptions {

  static class Builder {

    private OpType mOpType;

    private boolean mPartialListing;

    private boolean mVerify;

    private OptionalLong mBandwidth = OptionalLong.empty();

    private String mProgressFormat;

    private boolean mVerbose;

    private boolean mLoadMetadataOnly;

    private boolean mSkipIfExists;

    private Optional<String> mFileFilterRegx = Optional.empty();

    private Builder() {
    }

    /**
     * Set the operation type {@link OpType}.
     * @param opType the operation type
     */
    public void setOpType(OpType opType) {
      mOpType = opType;
    }

    /**
     * Set if partial listing.
     * @param partialListing if partial listing
     */
    public void setPartialListing(boolean partialListing) {
      mPartialListing = partialListing;
    }

    /**
     * Set whether we need to verify or not.
     * @param verify whether we needs to verify or not
     */
    public void setVerify(boolean verify) {
      mVerify = verify;
    }

    /**
     * Set the bandwidth.
     * @param bandwidth the bandwidth
     */
    public void setBandWidth(long bandwidth) {
      mBandwidth = OptionalLong.of(bandwidth);
    }

    /**
     * Set the output format of progress.
     * @param format the output format of progress
     */
    public void setProgressFormat(String format) {
      mProgressFormat = format;
    }

    /**
     * Set if print verbose information.
     * @param verbose if print verbose information
     */
    public void setVerbose(boolean verbose) {
      mVerbose = verbose;
    }

    /**
     * Set if load metadata only.
     * @param loadMetadataOnly if load metadata only
     */
    public void setLoadMetadataOnly(boolean loadMetadataOnly) {
      mLoadMetadataOnly = loadMetadataOnly;
    }

    /**
     * Set skip if exists.
     * @param skipIfExists skip if exists
     */
    public void setSkipIfExists(boolean skipIfExists) {
      mSkipIfExists = skipIfExists;
    }

    /**
     * Set the file filter regx pattern string.
     * @param fileFilterRegx the file filter regx pattern string
     */
    public void setFileFilterRegx(Optional<String> fileFilterRegx) {
      mFileFilterRegx = fileFilterRegx;
    }

    /**
     * Get the operation type {@link OpType}.
     * @return the operation type {@link OpType}
     */
    public OpType getOpType() {
      return mOpType;
    }

    /**
     * Whether this request wants partial listing.
     * @return whether this request wants partial listing or not
     */
    public boolean isPartialListing() {
      return mPartialListing;
    }

    /**
     * Whether this request requires verify or not.
     * @return whether this request requires verify or not
     */
    public boolean isVerify() {
      return mVerify;
    }

    /**
     * The bandwidth of the load job.
     * @return the bandwidth of the load job
     */
    public OptionalLong getBandwidth() {
      return mBandwidth;
    }

    /**
     * The exact response format when getting the progress.
     * @return tThe exact response format when getting the progress
     */
    public String getProgressFormat() {
      return mProgressFormat;
    }

    /**
     * Whether this request wants verbose information or not.
     * @return whether this request wants verbose information or not
     */
    public boolean isVerbose() {
      return mVerbose;
    }

    /**
     * Whether this request just wants to load the metadata or not.
     * @return whether this request just wants to load the metadata or not
     */
    public boolean isLoadMetadataOnly() {
      return mLoadMetadataOnly;
    }

    /**
     * Whether this request wants to skip the existing file or not.
     * @return whether this request wants to skip the existing file or not
     */
    public boolean isSkipIfExists() {
      return mSkipIfExists;
    }

    /**
     * Get the file filter regx pattern string.
     * @return the file filter regx pattern string
     */
    public Optional<String> getFileFilterRegx() {
      return mFileFilterRegx;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public HttpLoadOptions build() {
      return new HttpLoadOptions(mOpType, mPartialListing, mVerify, mBandwidth,
          mProgressFormat, mVerbose, mLoadMetadataOnly, mSkipIfExists, mFileFilterRegx);
    }
  }

  enum OpType {
    SUBMIT,
    STOP,
    PROGRESS;

    public static OpType of(String type) {
      switch (type.toLowerCase()) {
        case "submit":
          return SUBMIT;
        case "stop":
          return STOP;
        case "progress":
          return PROGRESS;
        default:
          throw new UnsupportedOperationException("Unsupported op type: " + type);
      }
    }
  }

  private final OpType mOpType;

  private final boolean mPartialListing;

  private final boolean mVerify;

  private final OptionalLong mBandwidth;

  private final String mProgressFormat;

  private final boolean mVerbose;

  private final boolean mLoadMetadataOnly;

  private final boolean mSkipIfExists;

  private final Optional<String> mFileFilterRegx;

  /**
   * Create an object of {@link HttpLoadOptions}. A data model for the HTTP load options.
   * @param opType the operation type
   * @param partialListing if we want to partial listing
   * @param verify if we need to verify
   * @param bandwidth the bandwidth
   * @param progressFormat the output format of progress
   * @param verbose if we want to print the verbose information
   * @param loadMetadataOnly if we load metadata only
   * @param skipIfExists skip if exists
   * @param fileFilterRegx the file filter regx pattern string
   */
  public HttpLoadOptions(OpType opType, boolean partialListing, boolean verify,
                         OptionalLong bandwidth, String progressFormat, boolean verbose,
                         boolean loadMetadataOnly, boolean skipIfExists,
                         Optional<String> fileFilterRegx) {
    mOpType = opType;
    mPartialListing = partialListing;
    mVerify = verify;
    mBandwidth = bandwidth;
    mProgressFormat = progressFormat;
    mVerbose = verbose;
    mLoadMetadataOnly = loadMetadataOnly;
    mSkipIfExists = skipIfExists;
    mFileFilterRegx = fileFilterRegx;
  }

  /**
   * Get the operation type {@link OpType}.
   * @return the operation type {@link OpType}
   */
  public OpType getOpType() {
    return mOpType;
  }

  /**
   * Whether this request wants partial listing.
   * @return whether this request wants partial listing or not
   */
  public boolean isPartialListing() {
    return mPartialListing;
  }

  /**
   * Whether this request requires verify or not.
   * @return whether this request requires verify or not
   */
  public boolean isVerify() {
    return mVerify;
  }

  /**
   * The bandwidth of the load job.
   * @return the bandwidth of the load job
   */
  public OptionalLong getBandwidth() {
    return mBandwidth;
  }

  /**
   * The exact response format when getting the progress.
   * @return tThe exact response format when getting the progress
   */
  public String getProgressFormat() {
    return mProgressFormat;
  }

  /**
   * Whether this request wants verbose information or not.
   * @return whether this request wants verbose information or not
   */
  public boolean isVerbose() {
    return mVerbose;
  }

  /**
   * Whether this request just wants to load the metadata or not.
   * @return whether this request just wants to load the metadata or not
   */
  public boolean isLoadMetadataOnly() {
    return mLoadMetadataOnly;
  }

  /**
   * Whether this request wants to skip the existing file or not.
   * @return whether this request wants to skip the existing file or not
   */
  public boolean isSkipIfExists() {
    return mSkipIfExists;
  }

  /**
   * Get the file filter regx pattern string.
   * @return the file filter regx pattern string
   */
  public Optional<String> getFileFilterRegx() {
    return mFileFilterRegx;
  }
}
