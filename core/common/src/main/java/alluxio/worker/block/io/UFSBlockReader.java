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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A reader interface to access the data of a block stored in managed storage.
 * <p>
 * This class does not provide thread-safety.
 */
public class UFSBlockReader implements BlockReader {

  /**
   * Constructs a Block reader given the file path of the block.
   *
   * @param path file path of the block
   * @throws IOException if its file can not be open with "r" mode
   */
  public UFSBlockReader(String path) throws IOException {
  }


  public ByteBuffer read(long offset, long length) throws IOException {

  }

  /**
   * Gets the length of the block in bytes.
   *
   * @return the length of the block in bytes
   */
  public long getLength() {

  }

  /**
   * Returns a readable byte channel of the block.
   *
   * @return channel
   */
  public ReadableByteChannel getChannel() {

  }

  public InputStream getInStream() {

  }
}
