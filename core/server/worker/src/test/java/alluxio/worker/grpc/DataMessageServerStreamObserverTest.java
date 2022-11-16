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

package alluxio.worker.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import alluxio.collections.ConcurrentIdentityHashMap;
import alluxio.grpc.BufferRepository;
import alluxio.grpc.DataMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;

import io.grpc.stub.CallStreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.Map;

public class DataMessageServerStreamObserverTest {
  @Test
  public void notLeakyOnException() {
    BufRepo bufRepo = new BufRepo();
    @SuppressWarnings("unchecked")
    CallStreamObserver<String> mockObserver = mock(CallStreamObserver.class);
    // correctly retrieve and release the buffer on first invocation as a normal observer will do
    doAnswer(invocation -> {
      bufRepo.pollBuffer(invocation.getArgument(0)).release();
      return null;
    }).when(mockObserver).onNext(eq("1"));
    // throw exception on second
    doThrow(new SpecialSpecialException()).when(mockObserver).onNext(eq("2"));

    DataMessageServerStreamObserver<String> observer =
        new DataMessageServerStreamObserver<>(mockObserver, bufRepo);
    assertEquals(0, bufRepo.getRepo().size());

    ByteBuf buf1 = Unpooled.buffer();
    observer.onNext(new DataMessage<>("1", new NettyDataBuffer(buf1)));
    assertEquals(0, bufRepo.getRepo().size());
    assertEquals(0, buf1.refCnt());

    ByteBuf buf2 = Unpooled.buffer();
    assertThrows(SpecialSpecialException.class,
        () -> observer.onNext(new DataMessage<>("2", new NettyDataBuffer(buf2))));
    assertEquals(0, bufRepo.getRepo().size());
    assertEquals(0, buf2.refCnt());
  }

  private static final class SpecialSpecialException extends RuntimeException {}

  private static class BufRepo implements BufferRepository<String, DataBuffer> {
    private final Map<String, DataBuffer> mRepo = new ConcurrentIdentityHashMap<>();

    @Override
    public void offerBuffer(DataBuffer buffer, String message) {
      mRepo.put(message, buffer);
    }

    @Override
    public DataBuffer pollBuffer(String message) {
      return mRepo.remove(message);
    }

    @Override
    public void close() {
      // correct impl should clear all lingering entries in mRepo
      // deliberately skip clearing for testing purpose
    }

    public Map<String, DataBuffer> getRepo() {
      return mRepo;
    }
  }
}
