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

import static org.junit.Assert.assertEquals;

import jnr.ffi.NativeType;
import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StructTest {

  static class DummyJniStruct extends Struct {
    public DummyJniStruct() {
      super(ByteBuffer.allocate(1));
      m_signed32 = new Signed32();
      m_long = new UnsignedLong();
      m_padding = new Padding(4);
      m_int64 = new u_int64_t();
    }

    public final Signed32 m_signed32;
    public final UnsignedLong m_long;
    public final Padding m_padding;
    public final u_int64_t m_int64;
  }

  static class DummyJnrStruct extends jnr.ffi.Struct {
    public DummyJnrStruct(jnr.ffi.Runtime runtime) {
      super(runtime);
      m_signed32 = new jnr.ffi.Struct.Signed32();
      m_long = new jnr.ffi.Struct.UnsignedLong();
      m_padding = new jnr.ffi.Struct.Padding(NativeType.UCHAR ,4);
      m_int64 = new jnr.ffi.Struct.u_int64_t();
    }
    public final jnr.ffi.Struct.Signed32 m_signed32;
    public final jnr.ffi.Struct.UnsignedLong m_long;
    public final jnr.ffi.Struct.Padding m_padding;
    public final jnr.ffi.Struct.u_int64_t m_int64;
  }

  @Test
  public void offset() {
    DummyJniStruct st = new DummyJniStruct();
    DummyJnrStruct jnrst = new DummyJnrStruct(Runtime.getSystemRuntime());
    assertEquals(jnrst.m_signed32.offset(), st.m_signed32.offset());
    assertEquals(jnrst.m_long.offset(), st.m_long.offset());
    // FIXME(iluoeli): ensure alignment consistency
    // assertEquals(jnrst.m_int64.offset(), st.m_int64.offset());
  }
}
