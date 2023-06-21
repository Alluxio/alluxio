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
 * An annotation that helps identify test code and test cases that need to be removed or fixed
 * during transitioning the code base to the new Dora architecture. In the end, all marked items
 * should be resolved, by when this annotation will not be used anywhere.
 *
 * Removed features/tests should be recycled from master-2.x branch, instead of the main branch.
 * This is because the master-2.x branch will be more up-to-date due to the following reasons:
 * 1. After this piece of code is removed from main, changes and fixes may be added to master-2.x.
 * 2. The code has been removed from main, that suggests it matters little. In that case,
 *    we deem it unlikely to have improvements/fixes in main but not in master-2.x.
 * 3. That code is tested in master-2.x and used in real environments. But in main branch
 *    probably no one uses it.
 *
 * That said, if you dig up a piece of code with this mark. Recycle it from the master-2.x branch.
 * https://github.com/Alluxio/alluxio/tree/master-2.x
 */
public @interface DoraTestTodoItem {
  /**
   * The owner of this marked item.
   *
   * @return the owner name
   */
  String owner();
  /**
   * The action the owner should take.
   *
   * @return the action
   */
  Action action();
  /**
   * Additional comments like why this item is marked.
   * @return comment contents
   */
  String comment() default "";

  /**
   * The action that owner should take on the item.
   */
  enum Action {
    // The owner should remove the marked item because it is no longer applicable
    REMOVE,
    // The owner should fix the marked item because it is still applicable, but the current
    // code is broken due to the transition
    FIX,
  }
}
