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
package alluxio.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementers of this interface provide a read API that writes to a ByteBuffer, not a byte[].
 */
public interface ByteBufferReadable {

  /**
   * Reads up to buf.remaining() bytes into buf. Callers should use buf.limit(..) to control the
   * size of the desired read.
   * <p/>
   * After a successful call, buf.position() and buf.limit() should be unchanged, and therefore any
   * data can be immediately read from buf. buf.mark() may be cleared or updated.
   * <p/>
   * In the case of an exception, the values of buf.position() and buf.limit() are undefined, and
   * callers should be prepared to recover from this eventuality.
   * <p/>
   *
   * @param buf the ByteBuffer to receive the results of the read operation. Up to buf.limit() -
   *            buf.position() bytes may be read.
   * @return the number of bytes available to read from buf
   * @throws IOException if there is some error performing the read
   */
  int read(ByteBuffer buf) throws IOException;
}
