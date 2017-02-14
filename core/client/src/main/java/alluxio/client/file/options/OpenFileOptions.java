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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for opening a file for reading.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class OpenFileOptions {
  private FileWriteLocationPolicy mLocationPolicy;
  private ReadType mReadType;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static OpenFileOptions defaults() {
    return new OpenFileOptions();
  }

  /**
   * Creates a new instance with defaults based on the configuration.
   */
  private OpenFileOptions() {
    mReadType =
        Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    try {
      mLocationPolicy = CommonUtils.createNewClassInstance(
          Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the location policy used when storing data to Alluxio
   */
  @JsonIgnore
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the location policy class used when storing data to Alluxio
   */
  public String getLocationPolicyClass() {
    return mLocationPolicy.getClass().getCanonicalName();
  }

  /**
   * @return the read type
   */
  public ReadType getReadType() {
    return mReadType;
  }

  /**
   * @param locationPolicy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  @JsonIgnore
  public OpenFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param className the location policy class to use when storing data to Alluxio
   * @return the updated options object
   */
  public OpenFileOptions setLocationPolicyClass(String className) {
    try {
      @SuppressWarnings("unchecked") Class<FileWriteLocationPolicy> clazz =
          (Class<FileWriteLocationPolicy>) Class.forName(className);
      mLocationPolicy = CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
      return this;
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return this;
  }

  /**
   * @param readType the {@link ReadType} for this operation
   * @return the updated options object
   */
  public OpenFileOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @return the {@link InStreamOptions} representation of this object
   */
  public InStreamOptions toInStreamOptions() {
    return InStreamOptions.defaults().setReadType(mReadType).setLocationPolicy(mLocationPolicy);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenFileOptions)) {
      return false;
    }
    OpenFileOptions that = (OpenFileOptions) o;
    return Objects.equal(mLocationPolicy, that.mLocationPolicy)
        && Objects.equal(mReadType, that.mReadType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLocationPolicy, mReadType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("locationPolicy", mLocationPolicy)
        .add("readType", mReadType)
        .toString();
  }
}
