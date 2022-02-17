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

import java.nio.ByteBuffer;

public class Fuse2FuseFileInfo extends FuseFileInfo {
  public final Signed32 flags;
  public final NumberField fh_old;
  //  public final Padding direct_io;
  //  public final Padding keep_cache;
  //  public final Padding flush;
  //  public final Padding nonseekable;
  //  public final Padding flock_release;
  public final Padding padding;
  public final u_int64_t fh;
  public final u_int64_t lock_owner;

  public Fuse2FuseFileInfo(Runtime runtime, ByteBuffer buffer) {
    super(runtime, buffer);
    this.flags = new Signed32();
    this.fh_old = new UnsignedLong();
//      this.direct_io = new Padding(this, NativeType.UCHAR, 1);
//      this.keep_cache = new Padding(this, NativeType.UCHAR, 1);
//      this.flush = new Padding(this, NativeType.UCHAR, 1);
//      this.nonseekable = new Padding(this, NativeType.UCHAR, 1);
//      this.flock_release = new Padding(this, NativeType.UCHAR, 1);
    this.padding = new Padding(NativeType.UCHAR, 4);
    this.fh = new u_int64_t();
    this.lock_owner = new u_int64_t();
  }
}
