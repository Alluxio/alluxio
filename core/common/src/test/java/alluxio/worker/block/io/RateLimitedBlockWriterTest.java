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
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RateLimitedBlockWriterTest {

  private RateLimitedBlockWriter mWriter;
  private MockBlockWriter mBlockWriter;
  private AlluxioConfiguration mConf;

  /**
   * Sets up the file path and file block reader before a test runs.
   */
  @Before
  public void before() throws Exception {
    InstancedConfiguration configuration = Configuration.copyGlobal();
    configuration.set(PropertyKey.WORKER_LOCAL_BLOCK_QOS_ENABLE, true);
    configuration.set(PropertyKey.WORKER_LOCAL_BLOCK_WRITE_THROUGHPUT, "1MB");
    mConf = configuration;
    mBlockWriter = new MockBlockWriter();
    mWriter = new RateLimitedBlockWriter(mBlockWriter, configuration);
  }

  @After
  public void after() throws IOException {
    mWriter.close();
  }

  @Test
  public void testSingleAppendByteBuf() throws IOException {
    long start = System.currentTimeMillis();
    long len = 17 * 1024 * 1024;
    while (len > 0) {
      ByteBuf buffer = Unpooled.wrappedBuffer(
          BufferUtils.getIncreasingByteBuffer(2048));
      long current = mWriter.append(buffer);
      len -= current;
      buffer.release();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testSingleAppendByteBuffer() throws IOException {
    long start = System.currentTimeMillis();
    long len = 17 * 1024 * 1024;
    while (len > 0) {
      ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
      long current = mWriter.append(buf);
      len -= current;
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testSingleAppendDataBuffer() throws IOException {
    long start = System.currentTimeMillis();
    long len = 17 * 1024 * 1024;
    while (len > 0) {
      ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
      DataBuffer dataBuffer = new NioDataBuffer(buf, 2048);
      long current = mWriter.append(dataBuffer);
      len -= current;
      dataBuffer.release();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testSingleChannel() throws IOException {
    long start = System.currentTimeMillis();
    long len = 17 * 1024 * 1024;
    WritableByteChannel channel = mWriter.getChannel();
    while (len > 0) {
      ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
      long current = channel.write(buf);
      len -= current;
    }
    channel.close();
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 15000);
  }

  @Test
  public void testMultiAppendByteBuf() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    MockBlockWriter blockWriter1 = new MockBlockWriter();
    BlockWriter writer1 = new RateLimitedBlockWriter(blockWriter1, mConf);
    List<Future<Boolean>> futureList = new ArrayList<>();
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuf buffer = Unpooled.wrappedBuffer(
            BufferUtils.getIncreasingByteBuffer(2048));
        long current = mWriter.append(buffer);
        len -= current;
        buffer.release();
      }
      return true;
    }));
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuf buffer = Unpooled.wrappedBuffer(
            BufferUtils.getIncreasingByteBuffer(2048));
        long current = writer1.append(buffer);
        len -= current;
        buffer.release();
      }
      return true;
    }));
    long start = System.currentTimeMillis();
    for (Future<Boolean> future : futureList) {
      future.get();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 30000);
  }

  @Test
  public void testMultiAppendByteBuffer() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    MockBlockWriter blockWriter1 = new MockBlockWriter();
    BlockWriter writer1 = new RateLimitedBlockWriter(blockWriter1, mConf);
    List<Future<Boolean>> futureList = new ArrayList<>();
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        long current = mWriter.append(buf);
        len -= current;
      }
      return true;
    }));
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        long current = writer1.append(buf);
        len -= current;
      }
      return true;
    }));
    long start = System.currentTimeMillis();
    for (Future<Boolean> future : futureList) {
      future.get();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 30000);
  }

  @Test
  public void testMultiAppendDataBuffer() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    MockBlockWriter blockWriter1 = new MockBlockWriter();
    BlockWriter writer1 = new RateLimitedBlockWriter(blockWriter1, mConf);
    List<Future<Boolean>> futureList = new ArrayList<>();
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        DataBuffer dataBuffer = new NioDataBuffer(buf, 2048);
        long current = mWriter.append(dataBuffer);
        len -= current;
        dataBuffer.release();
      }
      return true;
    }));
    futureList.add(executorService.submit(() -> {
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        DataBuffer dataBuffer = new NioDataBuffer(buf, 2048);
        long current = writer1.append(dataBuffer);
        len -= current;
        dataBuffer.release();
      }
      return true;
    }));
    long start = System.currentTimeMillis();
    for (Future<Boolean> future : futureList) {
      future.get();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 30000);
  }

  @Test
  public void testMultiChannel() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    MockBlockWriter blockWriter1 = new MockBlockWriter();
    BlockWriter writer1 = new RateLimitedBlockWriter(blockWriter1, mConf);
    List<Future<Boolean>> futureList = new ArrayList<>();
    futureList.add(executorService.submit(() -> {
      WritableByteChannel channel = mWriter.getChannel();
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        long current = channel.write(buf);
        len -= current;
      }
      return true;
    }));
    futureList.add(executorService.submit(() -> {
      WritableByteChannel channel = writer1.getChannel();
      long len = 16 * 1024 * 1024;
      while (len > 0) {
        ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(2048);
        long current = channel.write(buf);
        len -= current;
      }
      return true;
    }));
    long start = System.currentTimeMillis();
    for (Future<Boolean> future : futureList) {
      future.get();
    }
    long cost = System.currentTimeMillis() - start;
    Assert.assertTrue(cost >= 30000);
  }
}
