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

package alluxio.jnifuse.struct;

import jnr.ffi.Runtime;
import jnr.ffi.Struct;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public interface FuseFileInfo {

  Struct.u_int64_t fh();

  Struct.Signed32 flags();

  static FuseFileInfo of(ByteBuffer buffer) {
    Runtime runtime = Runtime.getSystemRuntime();
    Fuse3FuseFileInfo fi = new Fuse3FuseFileInfo(runtime, buffer);
    fi.useMemory(jnr.ffi.Pointer.wrap(runtime, buffer));
    return fi;
  }
}
