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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for reading a file.
 */
@PublicApi
@NotThreadSafe
public final class InStreamOptions {
  private FileWriteLocationPolicy mLocationPolicy;
  private ReadType mReadType;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private InStreamOptions() {
    mReadType =
        ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    try {
      mLocationPolicy =
          CommonUtils.createNewClassInstance(ClientContext.getConf()
                  .<FileWriteLocationPolicy>getClass(Constants.USER_FILE_WRITE_LOCATION_POLICY),
              new Class[]{}, new Object[]{});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mReadType.getAlluxioStorageType();
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  public InStreamOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mLocationPolicy = policy;
    return this;
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *        {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public InStreamOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("readType", mReadType)
        .add("locationPolicy", mLocationPolicy).toString();
  }
}
