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
 * Method option for reading a file.
 */
@PublicApi
public final class InStreamOptions {
  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private ReadType mReadType;

  private InStreamOptions() {
    mReadType =
        ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
  }

  /**
   * @return the Tachyon storage type
   */
  public TachyonStorageType getTachyonStorageType() {
    return mReadType.getTachyonStorageType();
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *                 {@link TachyonStorageType}.
   * @return the builder
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
    sb.append(")");
    return sb.toString();
  }
}
