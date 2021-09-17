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

package alluxio.jnifuse;

import alluxio.jnifuse.struct.FileStat;

import java.nio.ByteBuffer;

public class FuseFillDir {
  public static native int fill(long address, long bufaddr, String name, ByteBuffer stbuf, long off);

  public static int apply(long fillerAddr, long bufaddr, String name, FileStat stbuf, long off) {
    if (stbuf != null) {
      return fill(fillerAddr, bufaddr, name, stbuf.getBuffer(), off);
    } else {
      return fill(fillerAddr, bufaddr, name, null, off);
    }
  }

  static {
    LibFuse.loadLibrary();
  }
}
