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

package alluxio.cli;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An enum standing for a UFS type.
 * */
@Retention(RetentionPolicy.RUNTIME)
public @interface ApplicableUfsType {
  /**
   * Gets the value.
   *
   * @return the UFS type
   * */
  Type value() default Type.ALL;

  /**
   * UFS types.
   * */
  enum Type {
    ALL,
    HDFS,
    OBJECT
  }
}
