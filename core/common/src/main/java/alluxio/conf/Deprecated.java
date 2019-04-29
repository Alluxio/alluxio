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

package alluxio.conf;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation for PropertyKeys that can be used to mark a key as deprecated (slated for
 * removal). If a key is marked with this annotation, then any Alluxio processes should trigger
 * log warning messages upon configuration validation.
 *
 * This annotation is provided as an alternative to {@link java.lang.Deprecated}. Both
 * annotations do not need to be specified on a key. The advantage of using this annotation is
 * that we can supply a custom message rather than simply telling the user "This key has been
 * deprecated".
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Deprecated {
  /**
   * A message to the user which could suggest an alternative key.
   *
   * The message should not mention anything about deprecation, but rather alternatives,
   * side-effects, and/or the time-frame for removal of the key.
   *
   * An example may be:
   *
   * "This key is slated for removal in v2.0. An equivalent configuration property is
   * alluxio.x.y.z."
   *
   * or
   *
   * "This key is slated for removal in v3.0. Afterwards, this value will no longer be
   * configurable"
   *
   * @return A string explaining deprecation
   */
  String message() default "";
}
