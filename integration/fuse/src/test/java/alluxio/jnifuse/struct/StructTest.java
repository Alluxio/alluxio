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

import org.junit.Test;

import java.nio.ByteBuffer;

public class StructTest {

  static class DummyStruct extends Struct {

    public DummyStruct() {
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

  @Test
  public void offset() {
    DummyStruct st = new DummyStruct();
    assertEquals(0, st.m_signed32.offset());
    assertEquals(st.m_signed32.offset() + 4, st.m_long.offset());
    assertEquals(st.m_long.offset() + 8, st.m_padding.offset());
    assertEquals(st.m_padding.offset() + 4, st.m_int64.offset());
  }
}
