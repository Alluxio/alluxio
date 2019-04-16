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

import alluxio.Seekable;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * A seekable under file input stream wrapper that encapsulates the under file input stream.
 * Subclasses of this abstract class should implement the
 * {@link SeekableUnderFileInputStream#seek(long)} and the reposition the wrapped input stream.
 */
public abstract class SeekableUnderFileInputStream extends FilterInputStream implements Seekable {

  protected SeekableUnderFileInputStream(InputStream in) {
    super(in);
  }
}
