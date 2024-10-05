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
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.network.protocol.databuffer.managed.BufOwner;
import alluxio.network.protocol.databuffer.managed.BufferEnvelope;
import alluxio.network.protocol.databuffer.managed.OwnedByteBuf;
import alluxio.network.protocol.databuffer.managed.SharedByteBuf;
import alluxio.network.protocol.databuffer.managed.UnsafeOwnedByteBuf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

public class OwnedByteBufTest {
  @Test
  public void testOwnershipTransfer() {
    try (OwnedByteBuf<Marker> externalBuffer = new Producer().allocateFresh().unseal(new Marker());
        Modifier modifier = new Modifier(new Producer().allocateFresh())) {
      modifier.modifyInternal();
      modifier.modifyArgumentFromAnyOwner(externalBuffer.lend());
      new Consumer().consume(externalBuffer.send());
      Throwable thrown =
          assertThrows(IllegalStateException.class, externalBuffer::send);
      assertTrue(thrown.getMessage().contains(Marker.class.getName()));
    }
  }

  private static class Marker implements BufOwner<Marker> {
    @Override
    public Marker self() {
      return this;
    }

    @Override
    public void close() {}
  }

  /**
   * Producer of buffers.
   */
  private static class Producer {
    public BufferEnvelope allocateFresh() {
      return PooledDirectNioByteBuf.allocateManaged(Constants.KB);
    }

    // read from external data source, copy into new buffer and hand out
    public BufferEnvelope copyFrom(ByteBuf data) {
      // `data` may be shared by someone else
      // must do a copy into a freshly allocated buffer
      ByteBuf buf = Unpooled.buffer(data.readableBytes());
      buf.writeBytes(data);
      // this is safe as buf does not have any other references
      return UnsafeOwnedByteBuf.unsafeSeal(buf);
    }
  }

  private static class Modifier implements BufOwner<Modifier>, AutoCloseable {
    private final OwnedByteBuf<Modifier> mBuffer;

    /*
     * Most common use case for a constructor is to take a BufferEnvelope and immediately unseal
     * it.
     */
    public Modifier(BufferEnvelope envelope) {
      @SuppressWarnings("MustBeClosed")
      OwnedByteBuf<Modifier> buf = unseal(envelope);
      mBuffer = buf;
    }

    /*
     * Taking a readily available owned buffer is also an option.
     * This is less common, because the buffer is typed, to get the buffer from an envelope
     * you need a separate Modifier instance to turn an envelope into an owned buffer.
     */
    public Modifier(OwnedByteBuf<Modifier> buffer) {
      mBuffer = buffer;
    }

    public void modifyInternal() {
      SharedByteBuf<Modifier> buf = mBuffer.lend();
      // do stuff with buf
      // no need to anything special
    }

    public <OwnerT extends BufOwner<OwnerT>> void
        modifyArgumentFromAnyOwner(SharedByteBuf<OwnerT> buffer) {
      // do thing with buffer
      // no need to anything special
    }

    @Override
    public void close() {
      // must not forget to release owned buffer
      mBuffer.close();
    }

    @Override
    public Modifier self() {
      return this;
    }
  }

  private static class Consumer implements BufOwner<Consumer> {
    public void consume(BufferEnvelope envelope) {
      try (OwnedByteBuf<Consumer> buf = unseal(envelope)) {
        ByteBuf rawBuf = buf.unsafeUnwrap();
        // do stuff with rawBuf
        rawBuf.release();
      }
    }

    // this is bad practice
    public void directUse(OwnedByteBuf<Consumer> buffer) {
      // use buffer directly and forget to close it
    }

    // this method is taking a buffer not marked as owned by it
    // hopefully this can catch reviews' attention during code review
    public void useNotOwnedBuffer(OwnedByteBuf<Modifier> buffer) {
      // use buffer owned by someone else
    }

    @Override
    public Consumer self() {
      return this;
    }

    @Override
    public void close() {
      // not actually owned
    }
  }
}
