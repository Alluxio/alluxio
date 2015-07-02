/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This class provides write access to a temp block data file locally stored in managed storage.
 * <p>
 * This class does not provide thread-safety. Corresponding lock must be acquired.
 */
public class LocalFileBlockWriter implements BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();

  /**
   * Construct a Block writer given the metadata of a temp block
   *
   * @param tempBlockMeta metadata of this temp block
   * @throws IOException if its file can not be open with "rw" mode
   */
  public LocalFileBlockWriter(TempBlockMeta tempBlockMeta) throws IOException {
    this(Preconditions.checkNotNull(tempBlockMeta).getPath());
  }

  /**
   * Construct a Block writer given the file path of the block
   *
   * @param path file path of the block
   * @throws IOException if its file can not be open with "rw" mode
   */
  public LocalFileBlockWriter(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public WritableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    return write(mLocalFileChannel.size(), inputBuf);
  }

  /**
   * Writes data to the block from an input ByteBuffer.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf ByteBuffer that input data is stored in
   * @return the size of data that was written
   * @throws IOException
   */
  private long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit();
    ByteBuffer outputBuf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    outputBuf.put(inputBuf);
    CommonUtils.cleanDirectBuffer(outputBuf);
    return outputBuf.limit();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
