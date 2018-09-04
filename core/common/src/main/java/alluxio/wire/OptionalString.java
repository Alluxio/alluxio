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

import java.util.Optional;

/**
 * Wire type for an optional string.
 */
public final class OptionalString {
  private final Optional<String> mValue;

  /**
   * @param value a string optional
   */
  public OptionalString(Optional<String> value) {
    mValue = value;
  }

  private OptionalString(alluxio.thrift.OptionalString t) {
    mValue = Optional.ofNullable(t.getValue());
  }

  /**
   * @return the value
   */
  public Optional<String> getValue() {
    return mValue;
  }

  /**
   * @param t a thrift OptionalString
   * @return the corresponding wire type
   */
  public static OptionalString fromThrift(alluxio.thrift.OptionalString t) {
    return new OptionalString(t);
  }

  /**
   * @return the corresponding thrift OptionalString
   */
  public alluxio.thrift.OptionalString toThrift() {
    alluxio.thrift.OptionalString t = new alluxio.thrift.OptionalString();
    if (mValue.isPresent()) {
      t.setValue(mValue.get());
    }
    return t;
  }
}
