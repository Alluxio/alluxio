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

import java.nio.ByteBuffer;

public class FuseFileInfo extends Struct {

  public FuseFileInfo(ByteBuffer buffer) {
    super(buffer);
    flags = new Signed32();
    fh_old = new UnsignedLong();
    writepage = new Signed32();
    pad1 = new Padding(2);
    fh = new u_int64_t();
    lock_owner = new u_int64_t();
  }

  public final Signed32 flags;
  public final UnsignedLong fh_old;
  public final Signed32 writepage;
  public final Padding pad1;
  public final u_int64_t fh;
  public final u_int64_t lock_owner;

  public static FuseFileInfo wrap(ByteBuffer buffer) {
    return new FuseFileInfo(buffer);
  }
}
