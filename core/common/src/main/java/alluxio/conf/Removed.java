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
 * An annotation for PropertyKeys that can be used to mark a key as removed from use.
 *
 * The idea behind keeping the old property keys in the code base, but marking them as removed
 * allows us to warn or error for users who may have been using an older version of Alluxio, but
 * have upgraded to a more recent version. Instead of the user needing to scour documentation for
 * equivalent keys, we can provide a descriptive message to the user with information about what
 * has happened to the behavior or functionality of the property key.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Removed {
  /**
   * A message to the user which could suggest an alternative key, or reasons for the key's removal.
   *
   * The message should not mention that the key was removed, but rather alternatives,
   * side-effects, and/or the reason for removal of the key. Descriptions should be kept short
   * using 1-3 sentences.
   *
   * An example may be:
   *
   * "This key is being removed in favor of using key x.y.z. New optimizations render this
   * property obsolete."
   *
   * or
   *
   * "Internal architecture changes have rendered this key obsolete. There are no alternatives."
   *
   * @return A string explaining deprecation or removal
   */
  String message() default "";
}
