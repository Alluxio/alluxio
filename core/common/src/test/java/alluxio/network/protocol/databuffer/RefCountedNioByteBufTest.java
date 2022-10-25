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

package alluxio.network.protocol.databuffer;

import static org.junit.Assert.assertThrows;

import alluxio.Constants;

import io.netty.buffer.ByteBuf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.nio.ByteBuffer;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//    RefCountedNioByteBufTest.ByteBufTest.class,
    RefCountedNioByteBufTest.BasicTest.class
})
public class RefCountedNioByteBufTest {
//  public static class ByteBufTest extends io.netty.buffer.AbstractByteBufTest {
//    @Override
//    protected ByteBuf newBuffer(int capacity, int maxCapacity) {
//      // most test cases in the parent class want an unbounded buffer
//      // (maxCapacity is Integer.MAX_VALUE), but we cannot afford to upfront allocate a ByteBuffer
//      // that large, as ByteBuffer does not support dynamic expansion.
//      // at the time this test is created, the buffer can grow up to 64 MB in the test cases,
//      // so cap the max capacity to 64 MB.
//      maxCapacity = Math.min(64 * Constants.MB, maxCapacity);
//      return new LeakyByteBuf(ByteBuffer.allocate(maxCapacity), capacity, maxCapacity);
//    }
//  }

  public static class BasicTest {
    @Test
    public void invalidCapacity() {
      // capacity exceeds maxCapacity
      assertThrows(IllegalArgumentException.class,
          () -> new LeakyByteBuf(ByteBuffer.allocate(8), 8, 6));
      // maxCapacity exceeds backing storage's capacity
      assertThrows(IllegalArgumentException.class,
          () -> new LeakyByteBuf(ByteBuffer.allocate(8), 8, 10));
      // set capacity larger than max capacity
      ByteBuf buf = new LeakyByteBuf(ByteBuffer.allocate(8), 8, 8);
      assertThrows(IllegalArgumentException.class,
          () -> buf.capacity(10));
    }
  }

  private static class LeakyByteBuf extends RefCountedNioByteBuf {
    protected LeakyByteBuf(ByteBuffer buffer, int capacity, int maxCapacity) {
      super(buffer, capacity, maxCapacity);
    }

    @Override
    protected void deallocate() {
      // do nothing
    }
  }
}
