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

  /** Symbolic representation. */
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
