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

import jnr.ffi.NativeType;
import jnr.ffi.Runtime;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Maps to struct fuse_file_info in /usr/include/fuse/fuse_common.h
 */
public class Fuse2FuseFileInfo extends FuseFileInfo {

  // unused fields are omitted

  /**
   * Creates a FuseFileInfo class matching the struct fuse_file_info in libfuse2.
   *
   * This struct is not meant to be used directly.
   * You should use {@link alluxio.jnifuse.struct.FuseFileInfo#of(ByteBuffer)}
   * to create a FuseFileIfo that matches currently used libfuse.
   *
   * @param runtime the JNR runtime
   * @param buffer the ByteBuffer containing struct fuse_file_info from JNR
   */
  public Fuse2FuseFileInfo(Runtime runtime, ByteBuffer buffer) {
    super(runtime, buffer);

    this.flags = new Signed32();
    new UnsignedLong(); // fh_old
    new Padding(NativeType.UCHAR, 4); // unused flags and paddings
    this.fh = new u_int64_t();
    new u_int64_t(); // lock_owner
  }

  /**
   * Factory class to create {@link Fuse2FuseFileInfo}s.
   */
  public static class Factory implements FuseFileInfo.Factory {
    @Override
    public FuseFileInfo create(ByteBuffer buffer) {
      Runtime runtime = Runtime.getSystemRuntime();
      FuseFileInfo info = new Fuse2FuseFileInfo(runtime, buffer);
      info.useMemory(jnr.ffi.Pointer.wrap(runtime, buffer));
      return info;
    }
  }
}
