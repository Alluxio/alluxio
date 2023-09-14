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

package alluxio.worker.block.io;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RateLimitedBlockReaderTest {

  private RateLimitedBlockReader mReader;
  private MockBlockReader mBlockReader;
  private AlluxioConfiguration mConf;

  /**
   * Sets up the file path and file block reader before a test runs.
   */
  @Before
  public void before() throws Exception {
    InstancedConfiguration configuration = Configuration.copyGlobal();
    configuration.set(PropertyKey.WORKER_LOCAL_BLOCK_QOS_ENABLE, true);
    configuration.set(PropertyKey.WORKER_LOCAL_BLOCK_READ_THROUGHPUT, "1MB");
    mConf = configuration;
    mBlockReader = new MockBlockReader(new byte[16 * 1024 * 1024]);
    mReader = new RateLimitedBlockReader(mBlockReader, configuration);
  }

  @After
  public void after() throws IOException {
    mReader.close();
    Assert.assertTrue(mBlockReader.isClosed());
  }

  @Test
  public void testSingleReadLimit() throws Exception {
    long start = System.currentTimeMillis();
    long len = mReader.getLength();
    int currentOff = 0;
    while (len > 0) {
      int current = mReader.read(currentOff, 2048).remaining();
      len -= current;
      currentOff += current;
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testSingleTransLimit() throws Exception {
    long start = System.currentTimeMillis();
    long len = mReader.getLength();
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(2048, 2048);
    while (len > 0) {
      mReader.transferTo(buf);
      int current = buf.readableBytes();
      len -= current;
      if (buf.writableBytes() <= 0) {
        buf.clear();
      }
    }
    buf.release();
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testSingleChannelLimit() throws Exception {
    long start = System.currentTimeMillis();
    long len = mReader.getLength();
    ReadableByteChannel channel = mReader.getChannel();
    while (len > 0) {
      ByteBuffer buffer = ByteBuffer.allocate(2048);
      int current = channel.read(buffer);
      len -= current;
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testMultiReadLimit() throws Exception {
    MockBlockReader blockReader1 = new MockBlockReader(new byte[16 * 1024 * 1024]);
    RateLimitedBlockReader reader1 = new RateLimitedBlockReader(blockReader1, mConf);
    ExecutorService service = Executors.newFixedThreadPool(2);
    long start = System.currentTimeMillis();
    Future<Long> future1 = service.submit(() -> {
      long len = mReader.getLength();
      int currentOff = 0;
      while (len > 0) {
        int current = mReader.read(currentOff, 2048).remaining();
        len -= current;
        currentOff += current;
      }
      return null;
    });
    Future<Long> future2 = service.submit(() -> {
      long len = reader1.getLength();
      int currentOff = 0;
      while (len > 0) {
        int current = reader1.read(currentOff, 2048).remaining();
        len -= current;
        currentOff += current;
      }
      return null;
    });
    future1.get();
    future2.get();
    Assert.assertTrue(System.currentTimeMillis() - start >= 30000);
  }

  @Test
  public void testMultiTransLimit() throws Exception {
    MockBlockReader blockReader1 = new MockBlockReader(new byte[16 * 1024 * 1024]);
    RateLimitedBlockReader reader1 = new RateLimitedBlockReader(blockReader1, mConf);
    ExecutorService service = Executors.newFixedThreadPool(2);
    long start = System.currentTimeMillis();
    Future<Long> future1 = service.submit(() -> {
      long len = mReader.getLength();
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(2048, 2048);
      while (len > 0) {
        mReader.transferTo(buf);
        int current = buf.readableBytes();
        len -= current;
        if (buf.writableBytes() <= 0) {
          buf.clear();
        }
      }
      buf.release();
      return null;
    });
    Future<Long> future2 = service.submit(() -> {
      long len = reader1.getLength();
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(2048, 2048);
      while (len > 0) {
        reader1.transferTo(buf);
        int current = buf.readableBytes();
        len -= current;
        if (buf.writableBytes() <= 0) {
          buf.clear();
        }
      }
      buf.release();
      return null;
    });
    future1.get();
    future2.get();
    Assert.assertTrue(System.currentTimeMillis() - start >= 30000);
  }

  @Test
  public void testMultiChannelLimit() throws Exception {
    MockBlockReader blockReader1 = new MockBlockReader(new byte[16 * 1024 * 1024]);
    RateLimitedBlockReader reader1 = new RateLimitedBlockReader(blockReader1, mConf);
    ExecutorService service = Executors.newFixedThreadPool(2);
    long start = System.currentTimeMillis();
    Future<Long> future1 = service.submit(() -> {
      long len = mReader.getLength();
      ReadableByteChannel channel = mReader.getChannel();
      while (len > 0) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        int current = channel.read(buffer);
        len -= current;
      }
      return null;
    });
    Future<Long> future2 = service.submit(() -> {
      long len = reader1.getLength();
      ReadableByteChannel channel = reader1.getChannel();
      while (len > 0) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        int current = channel.read(buffer);
        len -= current;
      }
      return null;
    });
    future1.get();
    future2.get();
    Assert.assertTrue(System.currentTimeMillis() - start >= 30000);
  }
}
