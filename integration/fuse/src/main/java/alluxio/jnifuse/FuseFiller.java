/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.jnifuse;

import java.nio.ByteBuffer;

public class FuseFiller {
  long address;

  FuseFiller(long address) {
    this.address = address;
  }

  public native int doFill(long bufaddr, String name, ByteBuffer stbuf, long off);

  public int fill(long bufaddr, String name, FileStat stbuf, long off) {
    if (stbuf != null) {
      return doFill(bufaddr, name, stbuf.bb, off);
    } else {
      return doFill(bufaddr, name, null, off);
    }
  }

  static {
    System.loadLibrary("jnifuse");
  }

}
