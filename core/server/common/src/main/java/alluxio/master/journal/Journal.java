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

import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a journal.
 */
@ThreadSafe
public interface Journal {

  /**
   * Formats the journal.
   *
   * @return whether the format operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  boolean format() throws IOException;

  /**
   * @return the journal location
   */
  URI getLocation();

  /**
   * @return whether the journal has been formatted
   */
  boolean isFormatted() throws IOException;
}
