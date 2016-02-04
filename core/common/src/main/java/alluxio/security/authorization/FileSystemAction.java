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

package alluxio.security.authorization;

import javax.annotation.concurrent.ThreadSafe;

/**
 * POSIX style file system actions, e.g. read, write, etc.
 */
@ThreadSafe
public enum FileSystemAction {
  NONE("---"),
  EXECUTE("--x"),
  WRITE("-w-"),
  WRITE_EXECUTE("-wx"),
  READ("r--"),
  READ_EXECUTE("r-x"),
  READ_WRITE("rw-"),
  ALL("rwx");

  /** Symbolic representation */
  private final String mSymbol;

  /** Retain reference to value array. */
  private static final FileSystemAction[] SVALS = values();

  FileSystemAction(String s) {
    mSymbol = s;
  }

  /**
   * @return the symbolic representation of this action
   */
  public String getSymbol() {
    return mSymbol;
  }

  /**
   * Checks whether this action implies the passed-in action.
   *
   * @param that a passed-in action
   * @return true when this action implies the passed-in action
   */
  public boolean imply(FileSystemAction that) {
    if (that != null) {
      return (ordinal() & that.ordinal()) == that.ordinal();
    }
    return false;
  }

  /**
   * @param that a passed-in action
   * @return the intersection of this action and the passed-in action
   */
  public FileSystemAction and(FileSystemAction that) {
    if (that != null) {
      return SVALS[ordinal() & that.ordinal()];
    }
    throw new IllegalArgumentException("The passed-in Action cannot be null for [and] operation.");
  }

  /**
   * @param that a passed-in action
   * @return the union of this action and the passed-in action
   */
  public FileSystemAction or(FileSystemAction that) {
    if (that != null) {
      return SVALS[ordinal() | that.ordinal()];
    }
    throw new IllegalArgumentException("The passed-in Action cannot be null for [or] operation.");
  }

  /**
   * @return the complement of this action
   */
  public FileSystemAction not() {
    return SVALS[7 - ordinal()];
  }
}
