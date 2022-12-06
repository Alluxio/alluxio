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

import java.io.IOException;
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

  /**
   * The factory interface to create {@link FuseFileInfo}s.
   */
  public interface Factory {
    /**
     * Creates an instance of {@link FuseFileInfo}.
     *
     * @param buffer the byte buffer to wrap around
     * @return the created object
     */
    FuseFileInfo create(ByteBuffer buffer);
  }
}
