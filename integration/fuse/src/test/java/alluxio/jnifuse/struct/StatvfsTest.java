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

import static org.junit.Assert.assertEquals;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StatvfsTest {
  @Ignore
  @Test
  public void offset() {
    Statvfs jni = Statvfs.of(ByteBuffer.allocate(256));
    ru.serce.jnrfuse.struct.Statvfs jnr =
        ru.serce.jnrfuse.struct.Statvfs.of(Pointer.wrap(Runtime.getSystemRuntime(), 0x0));

    assertEquals(jnr.f_bsize.offset(), jni.f_bsize.offset());
    assertEquals(jnr.f_frsize.offset(), jni.f_frsize.offset());
    assertEquals(jnr.f_frsize.offset(), jni.f_frsize.offset());
    assertEquals(jnr.f_bfree.offset(), jni.f_bfree.offset());
    assertEquals(jnr.f_bavail.offset(), jni.f_bavail.offset());
    assertEquals(jnr.f_files.offset(), jni.f_files.offset());
    assertEquals(jnr.f_ffree.offset(), jni.f_ffree.offset());
    assertEquals(jnr.f_favail.offset(), jni.f_favail.offset());
    assertEquals(jnr.f_fsid.offset(), jni.f_fsid.offset());
    assertEquals(jnr.f_flag.offset(), jni.f_flag.offset());
    assertEquals(jnr.f_namemax.offset(), jni.f_namemax.offset());
  }
}
