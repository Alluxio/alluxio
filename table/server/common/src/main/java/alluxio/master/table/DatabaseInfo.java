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

package alluxio.master.table;

import alluxio.grpc.table.PrincipalType;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * The database information class.
 */
public class DatabaseInfo {
  @Nullable
  private final String mLocation;
  @Nullable
  private final String mOwnerName;
  @Nullable
  private final PrincipalType mOwnerType;
  @Nullable
  private final String mComment;
  private final Map<String, String> mParameters;

  /**
   * Full constructor for database info.
   * @param location location
   * @param ownerName owner name
   * @param ownerType owner type
   * @param comment comment
   * @param params parameters
   */
  public DatabaseInfo(String location, String ownerName, PrincipalType ownerType, String comment,
      Map<String, String> params) {
    mLocation = location;
    mOwnerName = ownerName;
    mOwnerType = ownerType;
    mComment = comment;
    if (params == null) {
      mParameters = Collections.emptyMap();
    } else {
      mParameters = params;
    }
  }

  /**
   * @return the location
   */
  public String getLocation() {
    return mLocation;
  }

  /**
   * @return the owner name
   */
  public String getOwnerName() {
    return mOwnerName;
  }

  /**
   * @return the owner type
   */
  public PrincipalType getOwnerType() {
    return mOwnerType;
  }

  /**
   * @return the comment
   */
  public String getComment() {
    return mComment;
  }

  /**
   * @return the parameter
   */
  public Map<String, String> getParameters() {
    return mParameters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabaseInfo that = (DatabaseInfo) o;
    return Objects.equals(mLocation, that.mLocation)
        && Objects.equals(mOwnerName, that.mOwnerName)
        && mOwnerType == that.mOwnerType
        && Objects.equals(mComment, that.mComment)
        && Objects.equals(mParameters, that.mParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mLocation, mOwnerName, mOwnerType, mComment, mParameters);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("location", mLocation)
        .add("ownerName", mOwnerName)
        .add("ownerType", mOwnerType)
        .add("comment", mComment)
        .add("parameters", mParameters)
        .toString();
  }
}
