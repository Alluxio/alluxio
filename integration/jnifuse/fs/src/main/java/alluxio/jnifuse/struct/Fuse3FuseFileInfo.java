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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Maps to struct fuse_file_info in /usr/include/fuse3/fuse_common.h
 */
public class Fuse3FuseFileInfo extends FuseFileInfo {

  // unused fields are omitted

  /**
   * Creates a FuseFileInfo class matching the struct fuse_file_info in libfuse3.
   *
   * This struct is not meant to be used directly.
   * You should use {@link alluxio.jnifuse.struct.FuseFileInfo#of(ByteBuffer)}
   * to create a FuseFileIfo that matches currently used libfuse.
   *
   * @param runtime the JNR runtime
   * @param buffer the ByteBuffer containing struct fuse_file_info from JNR
   */
  protected Fuse3FuseFileInfo(jnr.ffi.Runtime runtime, ByteBuffer buffer) {
    super(runtime, buffer);

    this.flags = new Signed32();
    new Padding(NativeType.UCHAR, 8); // unused flags and paddings
    this.fh = new u_int64_t();
    new u_int64_t(); // lock_owner
    new u_int32_t(); // poll_events
  }
}
