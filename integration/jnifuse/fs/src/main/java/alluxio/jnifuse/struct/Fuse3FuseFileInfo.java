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
import jnr.ffi.Struct;
import jnr.posix.util.Platform;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Fuse3FuseFileInfo extends Struct implements FuseFileInfo {

  private ByteBuffer buffer;

  private final Signed32 flags;

//  private final Padding writepage;
//
//  private final Padding direct_io;
//
//  private final Padding keep_cache;
//
//  private final Padding flush;
//
//  private final Padding nonseekable;
//
//  private final Padding flock_release;
//
//  private final Padding cache_readdir;
//
//  private final Padding padding;
//  private final Padding padding2;

  private final u_int64_t fh;

//  private final u_int64_t lock_owner;

//  private final u_int32_t poll_events;

  protected Fuse3FuseFileInfo(jnr.ffi.Runtime runtime, ByteBuffer buffer) {
    super(runtime);
    // TODO Windows support
    flags = new Signed32();
    // TODO map each bitfield. total length is 64bit
    new Padding(NativeType.UCHAR, 8);
//    writepage = new Padding(NativeType.UCHAR, 1);
//    direct_io = new Padding(NativeType.UCHAR, 1);
//    keep_cache = new Padding(NativeType.UCHAR, 1);
//    flush = new Padding(NativeType.UCHAR, 1);
//    nonseekable = new Padding(NativeType.UCHAR, 1);
//    flock_release = new Padding(NativeType.UCHAR, 1);
//    cache_readdir = new Padding(NativeType.UCHAR, 1);
//    padding = new Padding(NativeType.UCHAR, 25);
//    padding2 = new Padding(NativeType.UCHAR, 32);
    fh = new u_int64_t();
//    lock_owner = new u_int64_t();
//    poll_events = new u_int32_t();

    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public u_int64_t fh() {
    return this.fh;
  }

  @Override
  public Signed32 flags() {
    return this.flags;
  }


  public static Fuse3FuseFileInfo of(ByteBuffer buffer) {
    Runtime runtime = Runtime.getSystemRuntime();
    Fuse3FuseFileInfo fi = new Fuse3FuseFileInfo(runtime, buffer);
    fi.useMemory(jnr.ffi.Pointer.wrap(runtime, buffer));
    return fi;
  }

}
