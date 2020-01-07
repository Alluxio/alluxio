/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.PageStore;
import alluxio.resource.ResourcePool;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link LocalPageStore} is an implementation of {@link PageStore} which
 * stores all pages in a directory somewhere on the local disk.
 */
@NotThreadSafe
public class LocalPageStore implements PageStore, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPageStore.class);

  private final ResourcePool<ByteBuffer> mBuffers;

  private final String mRoot;

  /**
   * Creates a new instance of {@link LocalPageStore}.
   *
   * @param options options for the local page store
   */
  public LocalPageStore(LocalPageStoreOptions options) {
    mRoot = options.getRootDir();
    mBuffers = new ResourcePool<ByteBuffer>(options.getBufferPoolSize()) {
      @Override
      public void close() {
      }

      @Override
      protected ByteBuffer createNewResource() {
        return ByteBuffer.wrap(new byte[options.getBufferSize()]);
      }
    };
  }

  @Override
  public int put(long fileId, long pageIndex, ReadableByteChannel src) throws IOException {
    Path p = getFilePath(fileId, pageIndex);
    if (!Files.exists(p)) {
      Files.createDirectories(p.getParent());
      Files.createFile(p);
    }
    int written = 0;
    try (FileOutputStream fos = new FileOutputStream(p.toFile(), false)) {
      ByteBuffer b = mBuffers.acquire();
      b.clear();
      try (FileChannel chan = fos.getChannel()) {
        while (src.read(b) > 0) {
          b.flip();
          written += chan.write(b);
          b.clear();
        }
      } finally {
        mBuffers.release(b);
      }
    }
    return written;
  }

  @Override
  public int get(long fileId, long pageIndex, WritableByteChannel dst)throws IOException,
      PageNotFoundException {
    int count = 0;
    Path p = getFilePath(fileId, pageIndex);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    try (FileInputStream fis = new FileInputStream(p.toFile())) {
      ByteBuffer b = mBuffers.acquire();
      b.clear();
      try (FileChannel chan = fis.getChannel()) {
        while (chan.read(b) > 0) {
          b.flip();
          count += dst.write(b);
          b.clear();
        }
      } finally {
        mBuffers.release(b);
      }
    }
    return count;
  }

  @Override
  public void delete(long fileId, long pageIndex) throws IOException, PageNotFoundException {
    Path p = getFilePath(fileId, pageIndex);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    Files.delete(p);
    if (!Files.newDirectoryStream(p.getParent()).iterator().hasNext()) {
      Files.delete(p.getParent());
    }
  }

  private Path getFilePath(long fileId, long pageIndex) {
    return Paths.get(mRoot, Long.toString(fileId), Long.toString(pageIndex));
  }

  @Override
  public void close() {
    try {
      FileUtils.deleteDirectory(new File(mRoot));
    } catch (IOException e) {
      LOG.warn("Failed to clean up local page store directory", e);
    }
  }
}
