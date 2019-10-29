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

package alluxio.job.transform;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Metadata about a partition in Alluxio catalog service.
 */
public class PartitionInfo implements Serializable {
  private static final long serialVersionUID = 6905153658064056381L;

  private static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  private final String mSerdeClass;
  private final String mInputFormatClass;
  private final HashMap<String, String> mProperties;
  private final ArrayList<SchemaField> mFields;

  /**
   * @param serdeClass the full serde class name
   * @param inputFormatClass the full input format class name
   * @param properties the properties
   * @param fields the fields
   */
  public PartitionInfo(@JsonProperty("serdeClass") String serdeClass,
      @JsonProperty("inputFormatClass") String inputFormatClass,
      @JsonProperty("properties") HashMap<String, String> properties,
      @JsonProperty("fields") ArrayList<SchemaField> fields) {
    mSerdeClass = serdeClass;
    mInputFormatClass = inputFormatClass;
    mProperties = properties;
    mFields = fields;
  }

  /**
   * @return the format of the files in the partition
   */
  @JsonIgnore
  public Format getFormat() {
    // TODO
    return Format.PARQUET;
  }

  /**
   * @return the input format class name
   */
  public String getInputFormatClass() {
    return mInputFormatClass;
  }

  /**
   * @return the serde class name
   */
  public String getSerdeClass() {
    return mSerdeClass;
  }

  /**
   * @return the properties
   */
  public HashMap<String, String> getProperties() {
    return mProperties;
  }

  /**
   * @return the fields
   */
  public ArrayList<SchemaField> getFields() {
    return mFields;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PartitionInfo)) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) obj;
    return mSerdeClass.equals(that.mSerdeClass)
        && mInputFormatClass.equals(that.mInputFormatClass)
        && mProperties.equals(that.mProperties)
        && mFields.equals(that.mFields);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSerdeClass, mInputFormatClass, mProperties, mFields);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("serdeClass", mSerdeClass)
        .add("inputFormatClass", mInputFormatClass)
        .add("properties", mProperties)
        .add("fields", mFields)
        .toString();
  }
}
