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

package alluxio.job.plan.transform;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Metadata of a field in the schema.
 */
public class FieldSchema implements Serializable {
  private static final long serialVersionUID = 4573336558464588151L;

  private final int mId;
  private final String mName;
  private final String mType;
  private final String mComment;

  /**
   * @param id  the id
   * @param name the name
   * @param type the type
   * @param comment the comment
   */
  public FieldSchema(@JsonProperty("id") int id,
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("comment") String comment) {
    mId = id;
    mName = name;
    mType = type;
    mComment = comment;
  }

  /**
   * @return the ID of the field
   */
  public int getId() {
    return mId;
  }

  /**
   * @return the name of the field
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the type of the field
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the comment of the field
   */
  public String getComment() {
    return mComment;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FieldSchema)) {
      return false;
    }
    FieldSchema that = (FieldSchema) obj;
    return mId == that.mId
        && mName.equals(that.mName)
        && mType.equals(that.mType)
        && mComment.equals(that.mComment);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mName, mType, mComment);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId)
        .add("name", mName)
        .add("type", mType)
        .add("comment", mComment)
        .toString();
  }
}
