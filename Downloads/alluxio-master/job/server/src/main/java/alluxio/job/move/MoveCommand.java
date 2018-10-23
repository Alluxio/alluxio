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

package alluxio.job.move;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * A command telling a worker to move a file.
 */
public final class MoveCommand implements Serializable {
  private static final long serialVersionUID = -4287491133291080690L;

  private final String mSource;
  private final String mDestination;

  /**
   * @param source the source file to move
   * @param destination the destination file to move it to
   */
  public MoveCommand(String source, String destination) {
    mSource = source;
    mDestination = destination;
  }

  /**
   * @return the source
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the destination
   */
  public String getDestination() {
    return mDestination;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MoveCommand)) {
      return false;
    }
    MoveCommand that = (MoveCommand) o;
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .toString();
  }
}
