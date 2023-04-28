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

import alluxio.jnifuse.utils.NativeLibraryLoader;

import jnr.ffi.Runtime;
import jnr.ffi.Struct;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FuseFileInfo extends Struct {
  public ByteBuffer buffer;

  public u_int64_t fh;
  public Signed32 flags;

  public FuseFileInfo(Runtime runtime, ByteBuffer buffer) {
    super(runtime);
    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  public static FuseFileInfo of(ByteBuffer buffer) {
    Runtime runtime = Runtime.getSystemRuntime();
    // select the actual FuseFileInfo by loaded version
    NativeLibraryLoader.LoadState state = NativeLibraryLoader.getLoadState();

    // Ensure the lib has been loaded in testing
    // This is not possible when running a real cluster
    if (state == NativeLibraryLoader.LoadState.NOT_LOADED) {
      throw new RuntimeException("NativeLibraryLoader is not loaded");
    }
    FuseFileInfo fi = state == NativeLibraryLoader.LoadState.LOADED_2
        ? new Fuse2FuseFileInfo(runtime, buffer)
        : new Fuse3FuseFileInfo(runtime, buffer);
    fi.useMemory(jnr.ffi.Pointer.wrap(runtime, buffer));
    return fi;
  }
}
