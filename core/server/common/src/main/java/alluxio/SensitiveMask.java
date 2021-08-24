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

package alluxio;

/**
 * Mask some sensitive information.
 */
public interface SensitiveMask {

  /**
   * mask sensitive information and output a string for log.
   * @param args the object to be masked and printed as a string
   * @return a string with sentitive information masked
   */
  public String maskAndToString(Object... args);
}
