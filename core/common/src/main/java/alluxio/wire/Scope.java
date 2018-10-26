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

package alluxio.wire;

/**
 * Scope where the property applies to.
 */
public enum Scope {
  /**
   * Property is used in masters.
   */
  MASTER(0b1),
  /**
   * Property is used in workers.
   */
  WORKER(0b10),
  /**
   * Property is used in clients.
   */
  CLIENT(0b100),
  /**
   * Property is used in masters and workers.
   */
  SERVER(0b11),
  ALL(0b111),
  NONE(0b0)
  ;

  private final int mValue;
  Scope(int val) {
    mValue = val;
  }

  /**
   * Check whether the scope contains another scope.
   * @param scope the target scope to check
   * @return true if the scope contains the target scope
   */
  public boolean contains(Scope scope) {
    return (mValue | scope.mValue) == mValue;
  }

  /**
   * @return the thrift representation of this scope field
   */
  public alluxio.thrift.Scope toThrift() {
    return alluxio.thrift.Scope.valueOf(name());
  }

  /**
   * @param field the thrift representation of the scope field to create
   * @return the wire type version of the scope field
   */
  public static Scope fromThrift(alluxio.thrift.Scope field) {
    return Scope.valueOf(field.name());
  }
}
