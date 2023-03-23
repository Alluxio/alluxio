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

package alluxio.master.journal.checkpoint;

import alluxio.util.FormatUtils;

import net.jpountz.lz4.LZ4FrameOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

/**
 * OutputStream to write checkpoint files efficiently.
 */
public class OptimizedCheckpointOutputStream extends OutputStream {
  public static final int BUFFER_SIZE = (int) FormatUtils.parseSpaceSize("4MB");

  private final OutputStream mOutputStream;

  /**
   * @param file where the checkpoint will be written
   * @param digest to ensure uncorrupted data
   * @throws IOException propagates wrapped output stream exceptions
   */
  public OptimizedCheckpointOutputStream(File file, MessageDigest digest) throws IOException {
    this(file, digest, BUFFER_SIZE);
  }

  /**
   * Constructor used for benchmarking.
   * @param file where the checkpoint will be written
   * @param digest to ensure uncorrupted data
   * @param bufferSize the buffer size that the output stream should use
   */
  public OptimizedCheckpointOutputStream(File file, MessageDigest digest, int bufferSize)
      throws IOException {
    mOutputStream = new DigestOutputStream(new LZ4FrameOutputStream(
        new BufferedOutputStream(Files.newOutputStream(file.toPath()), bufferSize)),
        digest);
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
  }
}
