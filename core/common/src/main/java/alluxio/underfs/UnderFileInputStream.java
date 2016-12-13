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

package alluxio.underfs;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper over an {@link InputStream} from an {@link UnderFileSystem}.
 */
@NotThreadSafe
public abstract class UnderFileInputStream extends InputStream {

  /**
   * Seek to the given offset from the start of the file. The next read() will be from that
   * location. Can't seek past the end of the file.
   *
   * @param position offset from the start of the file in bytes
   * @throws IOException if position is negative, or if a non-Alluxio error occurs
   */
  public abstract void seek(long position) throws IOException;
}
