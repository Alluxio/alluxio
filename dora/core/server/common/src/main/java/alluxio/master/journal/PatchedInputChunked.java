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

package alluxio.master.journal;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.InputChunked;

import java.io.InputStream;

/**
 * A replacement for Kryo's {@link InputChunked} which properly handles eof.
 *
 * See https://github.com/EsotericSoftware/kryo/issues/651.
 *
 * Once kryo releases 4.0.3, we can upgrade and remove this class.
 */
public class PatchedInputChunked extends InputChunked {
  /**
   * @param input the input stream
   */
  public PatchedInputChunked(InputStream input) {
    super(input);
  }

  @Override
  protected int fill(byte[] buffer, int offset, int count) throws KryoException {
    try {
      return super.fill(buffer, offset, count);
    } catch (KryoException e) {
      if (e.getMessage().equals("Buffer underflow.")) {
        return -1;
      }
      throw e;
    }
  }
}
