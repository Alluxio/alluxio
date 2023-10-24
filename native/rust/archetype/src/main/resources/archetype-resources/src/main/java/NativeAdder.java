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

package ${package};

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * Native adder.
 */
public class NativeAdder {

  interface RustAdder extends Library {
    RustAdder INSTANCE = Native.load("${cargo_crate_name}", RustAdder.class);

    int add(int a, int b);
  }

  /**
   * Adds two integers.
   *
   * @param a integer a
   * @param b integer b
   * @return the sum
   */
  public int add(int a, int b) {
    return RustAdder.INSTANCE.add(a, b);
  }
}
