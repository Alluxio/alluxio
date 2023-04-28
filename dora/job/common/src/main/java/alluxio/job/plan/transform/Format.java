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

/**
 * Supported formats.
 */
public enum Format {
  CSV(".csv"),
  GZIP_CSV(".csv.gz"),
  GZIP(".gz"),
  ORC(".orc"),
  PARQUET(".parquet");

  private String mSuffix;

  /**
   * @param suffix the suffix of the format
   */
  Format(String suffix) {
    mSuffix = suffix;
  }

  /**
   * @return the suffix of filename for the format
   */
  public String getSuffix() {
    return mSuffix;
  }

  /**
   * @param path the file path
   * @return whether the path points to a (compressed) CSV file
   */
  public static boolean isCsv(String path) {
    return path.endsWith(CSV.getSuffix()) || path.endsWith(GZIP_CSV.getSuffix());
  }

  /**
   * @param path the file path
   * @return whether the path points to a parquet file
   */
  public static boolean isParquet(String path) {
    return path.endsWith(PARQUET.getSuffix());
  }

  /**
   * @param path the file path
   * @return whether the path points to a gzipped file
   */
  public static boolean isGzipped(String path) {
    return path.endsWith(GZIP.getSuffix());
  }

  /**
   * @param path the file path
   * @return the format of the file
   */
  public static Format of(String path) {
    if (path.endsWith(CSV.getSuffix())) {
      return CSV;
    }
    if (path.endsWith(GZIP_CSV.getSuffix())) {
      return GZIP_CSV;
    }
    if (path.endsWith(PARQUET.getSuffix())) {
      return PARQUET;
    }
    throw new RuntimeException("Unsupported file format for " + path);
  }
}
