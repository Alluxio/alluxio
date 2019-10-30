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

package alluxio.job.transform.format.csv;

/**
 * Logical decimal type in Parquet.
 */
public class Decimal {
  private final int mPrecision;
  private final int mScale;

  /**
   * @param type the type definition, like "decimal(10, 2)"
   */
  public Decimal(String type) {
    String param = type.substring(8, type.length() - 1);
    String[] params = param.split(",");
    mPrecision = Integer.parseInt(params[0].trim());
    mScale = Integer.parseInt(params[1].trim());
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return mPrecision;
  }

  /**
   * @return the scale
   */
  public int getScale() {
    return mScale;
  }
}
