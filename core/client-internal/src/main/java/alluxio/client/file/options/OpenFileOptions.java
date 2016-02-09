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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for opening a file for reading.
 */
@PublicApi
@NotThreadSafe
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
  public OpenFileOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mLocationPolicy = policy;
    return this;
  }

  /**
   * @param readType the {@link ReadType} for this operation. Setting this will
   *        override the {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public OpenFileOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @return the {@link OutStreamOptions} representation of this object
   */
  public InStreamOptions toInStreamOptions() {
    return InStreamOptions.defaults().setReadType(mReadType).setLocationPolicy(mLocationPolicy);
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OpenFileOptions(");
    sb.append(super.toString()).append(", ReadType: ").append(mReadType.toString());
    sb.append(", LocationPolicy: ").append(mLocationPolicy.toString());
    sb.append(")");
    return sb.toString();
  }
}
