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
  long address;

  FuseFillDir(long address) {
    this.address = address;
  }

  public native int fill(long address, long bufaddr, String name, ByteBuffer stbuf, long off);

  public int apply(long bufaddr, String name, FileStat stbuf, long off) {
    if (stbuf != null) {
      return fill(address, bufaddr, name, stbuf.getBuffer(), off);
    } else {
      return fill(address, bufaddr, name, null, off);
    }
  }

  static {
    System.loadLibrary("jnifuse");
  }

}
