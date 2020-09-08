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

package alluxio.master.transport.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Handles parsing and writing of prefixes for serialization.
 */
enum Identifier {
  ID(0x01) {
    @Override
    public boolean accept(int id) {
      return true;
    }

    @Override
    public void write(int id, DataOutputStream output) throws IOException {
      output.writeInt(id);
    }

    @Override
    public int read(DataInputStream input) throws IOException {
      return input.readInt();
    }
  },

  NULL(0x00) {
    @Override
    public boolean accept(int id) {
      return false;
    }

    @Override
    public void write(int id, DataOutputStream output) throws IOException  {
    }

    @Override
    public int read(DataInputStream input) throws IOException {
      return 0;
    }
  },

  CLASS(0x03) {
    @Override
    public boolean accept(int id) {
      return false;
    }

    @Override
    public void write(int id, DataOutputStream output) throws IOException {
    }

    @Override
    public int read(DataInputStream input) throws IOException {
      return 0;
    }
  };

  private final int mCode;

  /**
   * Constructs a identifier with code.
   *
   * @param code the code to construct
   */
  Identifier(int code) {
    mCode = code;
  }

  /**
   * Returns the identifier for the given code.
   *
   * @param code The code for which to return the identifier
   * @return The identifier for the given code
   * @throws IllegalArgumentException if the code is not a valid identifier code
   */
  public static Identifier forCode(int code) {
    switch (code) {
      case 0x00:
        return NULL;
      case 0x01:
        return ID;
      case 0x02:
        return CLASS;
      default:
        throw new IllegalArgumentException("invalid code: " + code);
    }
  }

  /**
   * Returns the code for the identifier.
   */
  public int code() {
    return mCode;
  }

  /**
   * Checks if the given id falls into one given category.
   *
   * @param id the id to check
   * @return true if the given id belongs to the given category
   */
  public abstract boolean accept(int id);

  /**
   * Writes the id into the output.
   *
   * @param id the id to write
   * @param output the stream to write output to
   */
  public abstract void write(int id, DataOutputStream output) throws IOException;

  /**
   * Reads from the given input stream.
   *
   * @param input the stream to read from
   * @return the id value
   */
  public abstract int read(DataInputStream input) throws IOException;
}
