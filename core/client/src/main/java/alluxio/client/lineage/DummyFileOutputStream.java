/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.OutStreamOptions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A dummy file output stream that does nothing. This is used for lineage recomputation. When the
 * file is not lost on Alluxio, there's no need for the job to rewrite the file.
 */
@NotThreadSafe
public final class DummyFileOutputStream extends FileOutStream {

  /**
   * Constructs a new dummy file output stream.
   *
   * @param path the path of the file
   * @param options the set of options specific to this operation
   * @throws IOException if an I/O error occurs
   */
  public DummyFileOutputStream(AlluxioURI path, OutStreamOptions options) throws IOException {
    super(path, options);
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void write(int b) throws IOException {}

  @Override
  public void write(byte[] b) throws IOException {}

  @Override
  public void write(byte[] b, int off, int len) throws IOException {}

  @Override
  public void close() throws IOException {}
}
