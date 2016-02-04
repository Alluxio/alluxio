/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.file.options;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.AlluxioStorageType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

/**
 * Method option for reading a file.
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
   * @return the location policy to use when storing data to Tachyon
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Tachyon storage type
   */
  public AlluxioStorageType getTachyonStorageType() {
    return mReadType.getTachyonStorageType();
  }

  /**
   * @param policy the location policy to use when storing data to Tachyon
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
   *                 {@link AlluxioStorageType}.
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
    StringBuilder sb = new StringBuilder("InStreamOptions(");
    sb.append(super.toString()).append(", ReadType: ").append(mReadType.toString());
    sb.append(", LocationPolicy: ").append(mLocationPolicy.toString());
    sb.append(")");
    return sb.toString();
  }
}
