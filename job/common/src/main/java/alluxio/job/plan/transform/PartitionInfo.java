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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Metadata about a partition in Alluxio catalog service.
 */
public class PartitionInfo implements Serializable {
  private static final long serialVersionUID = 6905153658064056381L;

  /**
   * Key in Serde Properties to denote parquet compression method.
   */
  public static final String PARQUET_COMPRESSION = "file.parquet.compression";

  private final String mSerdeClass;
  private final String mInputFormatClass;
  private final HashMap<String, String> mSerdeProperties;
  private final HashMap<String, String> mTableProperties;
  private final ArrayList<FieldSchema> mFields;

  /**
   * @param serdeClass the full serde class name
   * @param inputFormatClass the full input format class name
   * @param serdeProperties the serde Properties
   * @param tableProperties the table Properties
   * @param fields the fields
   */
  public PartitionInfo(@JsonProperty("serdeClass") String serdeClass,
      @JsonProperty("inputFormatClass") String inputFormatClass,
      @JsonProperty("serdeProperties") HashMap<String, String> serdeProperties,
      @JsonProperty("tableProperties") HashMap<String, String> tableProperties,
      @JsonProperty("fields") ArrayList<FieldSchema> fields) {
    mSerdeClass = serdeClass;
    mInputFormatClass = inputFormatClass;
    mSerdeProperties = serdeProperties;
    mTableProperties = tableProperties;
    mFields = fields;
  }

  /**
   * @param filename the filename
   * @return the format of the files in the partition
   * @throws IOException when failed to determine format
   */
  @JsonIgnore
  public Format getFormat(String filename) throws IOException {
    if (mSerdeClass.equals(HiveConstants.PARQUET_SERDE_CLASS)) {
      return Format.PARQUET;
    } else if (mSerdeClass.equals(HiveConstants.CSV_SERDE_CLASS)
        || (mInputFormatClass.equals(HiveConstants.TEXT_INPUT_FORMAT_CLASS)
        && mSerdeProperties.containsKey(HiveConstants.SERIALIZATION_FORMAT))) {
      if (filename.endsWith(Format.GZIP.getSuffix())) {
        return Format.GZIP_CSV;
      }
      return Format.CSV;
    } else if (mSerdeClass.equals(HiveConstants.ORC_SERDE_CLASS)) {
      return Format.ORC;
    }
    // failed to get format from serde info, try to get it from extension
    if (filename.endsWith(Format.CSV.getSuffix())) {
      return Format.CSV;
    }
    if (filename.endsWith(Format.PARQUET.getSuffix())) {
      return Format.PARQUET;
    }
    // both method failed, throw exception
    throw new IOException("Cannot determine format for " + filename);
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
   * @return the serde properties
   */
  public HashMap<String, String> getSerdeProperties() {
    return mSerdeProperties;
  }

  /**
   * @return the table properties
   */
  public HashMap<String, String> getTableProperties() {
    return mTableProperties;
  }

  /**
   * @return the fields
   */
  public ArrayList<FieldSchema> getFields() {
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
        && mSerdeProperties.equals(that.mSerdeProperties)
        && mTableProperties.equals(that.mTableProperties)
        && mFields.equals(that.mFields);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSerdeClass, mInputFormatClass, mSerdeProperties, mTableProperties,
        mFields);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("serdeClass", mSerdeClass)
        .add("inputFormatClass", mInputFormatClass)
        .add("serdeProperties", mSerdeProperties)
        .add("tableProperties", mTableProperties)
        .add("fields", mFields)
        .toString();
  }
}
