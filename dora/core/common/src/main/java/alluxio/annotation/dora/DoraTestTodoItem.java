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

package alluxio.annotation.dora;

/**
 * Informational annotation to help identify things that needs to be removed or fixed
 * during the Dora architecture transition.
 *
 * Removed features should be recycled from master-2.x branch, instead of the main branch.
 */
public @interface DoraTestTodoItem {
  Action action();
  String owner();
  String comment() default "";

  public enum Action {
    REMOVE,
    FIX,
  }
}
