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

package tachyon.client.file.options;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.TachyonStorageType;

/**
 * Method options for opening a file for reading.
 */
@PublicApi
public final class OpenFileOptions {
  /**
   * @return the default {@link InStreamOptions}
   */
  public static OpenFileOptions defaults() {
    return new OpenFileOptions();
  }

  private ReadType mReadType;

  /**
   * Creates a new instance with defaults based on the configuration
   */
  private OpenFileOptions() {
    mReadType =
        ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
  }

  /**
   * @param readType the {@link tachyon.client.ReadType} for this operation. Setting this will
   *        override the TachyonStorageType.
   * @return the builder
   */
  public OpenFileOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @return the Tachyon storage type
   */
  public TachyonStorageType getTachyonStorageType() {
    return mReadType.getTachyonStorageType();
  }

  /**
   * @return the {@link OutStreamOptions} representation of this object
   */
  public InStreamOptions toInStreamOptions() {
    return InStreamOptions.defaults().setReadType(mReadType);
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OpenFileOptions(");
    sb.append(super.toString()).append(", ReadType: ").append(mReadType.toString());
    sb.append(")");
    return sb.toString();
  }
}
