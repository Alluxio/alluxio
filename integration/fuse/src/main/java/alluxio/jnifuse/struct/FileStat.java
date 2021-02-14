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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FileStat extends ru.serce.jnrfuse.struct.FileStat {

  private final ByteBuffer buffer;

  protected FileStat(Runtime runtime, ByteBuffer buffer) {
    super(runtime);
    this.buffer = buffer;
    // depends on the arch
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public static FileStat of(ByteBuffer buffer) {
    Runtime runtime = Runtime.getSystemRuntime();
    FileStat stat = new FileStat(runtime, buffer);
    stat.useMemory(jnr.ffi.Pointer.wrap(runtime, buffer));
    return stat;
  }
}
