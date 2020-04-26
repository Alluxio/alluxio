/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.jnifuse;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class FileInfo {
  final ByteBuffer bb;

  /** A mask for the read/write flags **/
  public final static int O_ACCMODE = 0003;
  /** open for reading only **/
  public final static int O_RDONLY = 00;
  /** open for writing only **/
  public final static int O_WRONLY = 01;
  /** open for reading and writing **/
  public final static int O_RDWR = 02;
  /**
   * If the file exists, this flag has no effect except as noted under O_EXCL. Otherwise the file is
   * created; the user ID of the file is set to the effective user ID of the process; the group ID
   * of the file is set to the group ID of the file's parent directory or to the effective group ID
   * of the process; and the access permission bits (see sys/stat.h) of the file mode are set to the
   * value of the third argument taken as type mode_t (int) modified as follows: a bitwise-AND is
   * performed on the file-mode bits and the corresponding bits in the comlement of the process'
   * file mode creation mask. Thus all bits in the file mode whose corresponding bit in the file
   * mode creating mask is set are cleared. When bits other than the file permission bits are set,
   * the effect is unspecified. The third argument does not affect whether the file is open for
   * reading, writing or both.
   */
  public final static int O_CREAT = 0100;
  /**
   * If O_CREAT and O_EXCL are set, open() will fail if the file exists. The check for the existence
   * of the file and the creation of the file if it does not exist will be atomic with respect to
   * other processes executing open() nameing the same filename in the same directory with O_EXCL
   * and O_CREAT set. If O_CREAT is not set, the effect is undefined.
   */
  public final static int O_EXCL = 0200;
  /**
   * If set and path identifies a terminal device, open() will not cause the terminal device to
   * become the controlling terminal for the process
   **/
  public final static int O_NOCTTY = 0400;
  /**
   * If the file exists and is a regular file and the file is successfully opened O_RDWR or
   * O_WRONLY, its length is truncated to 0 and the mode and owner are unchanged. It will have no
   * effect on FIFO special files or terminal device files. Its effect on other types of is
   * implementation-dependent. The result of using O_TRUNC with O_RDONLY is undefined. If O_TRUNC is
   * set and the file did previously exist, upon successful comlpetion, open() will mark for update
   * the st_ctime and st_mtime fields of the file
   **/
  public final static int O_TRUNC = 01000;
  public final static int O_APPEND = 02000;
  /**
   * When opening a FIFO with O_RDONLY or O_WRONLY set, if O_NONBLOCK is set, an open() for reading
   * only will return without delay. An open() for writing only will return an error if no process
   * currently has the file open for reading. If O_NONBLOCK is clear, an open() for reading only
   * will block the calling thread until a thread opens the file for writing. An open() for writing
   * only will block the calling thread until a thread opens the file for reading. When opening a
   * block special or character special file that supports non-blocking opens: If O_NONBLOCK is set,
   * the open() function will return without blocking for the device to be ready or available;
   * subsequent behavior of the device is device-specific If O_NONBLOCK is clear, the open()
   * function will block the calling thread until the device is ready or available before returning.
   * Otherwise, the behavior of O_NONBLOCK is unspecified.
   */
  public final static int O_NONBLOCK = 04000;
  /** see O_NONBLOCK **/
  public final static int O_NDELAY = O_NONBLOCK;
  /**
   * Write I/O operations on the file descriptor complete as defined by synchronous I/O file
   * integrity completion
   **/
  public final static int O_SYNC = 04010000;
  /** see O_SYNC **/
  public final static int O_FSYNC = O_SYNC;
  public final static int O_ASYNC = 020000;

  /* Must be a directory. */
  public final static int O_DIRECTORY = 0200000;
  /* Do not follow links. */
  public final static int O_NOFOLLOW = 0400000;
  /* Set close_on_exec. */
  public final static int O_CLOEXEC = 02000000;
  /* Direct disk access. */
  public final static int O_DIRECT = 040000;
  /* Do not set atime. */
  public final static int O_NOATIME = 01000000;

  /**
   * Write I/O operations on the file descriptor complete as defined by the synchronized I/O data
   * integrity completion
   **/
  public final static int O_DSYNC = 010000;
  /** see O_SYNC **/
  public final static int O_RSYNC = O_SYNC;

  public final static int O_LARGEFILE = 0100000;


  public FileInfo(ByteBuffer bb) {
    this.bb = bb;
    this.bb.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Get Open flags. Available in open() and release()
   */
  public int getOpenFlags() {
    return bb.getInt(0x0);
  }

  /**
   * Set Open flags. Available in open() and release()
   */
  public FileInfo putOpenFlags(final int value) {
    bb.putInt(0x0, value);
    return this;
  }

  /**
   * Get In case of a write operation indicates if this was caused by a writepage
   */
  public int getWritePage() {
    return bb.getInt(0x10);
  }

  /**
   * Set In case of a write operation indicates if this was caused by a writepage
   */
  public FileInfo putWritePage(final int value) {
    bb.putInt(0x10, value);
    return this;
  }

  /**
   * Get File handle. May be filled in by filesystem in open(). Available in all other file
   * operations
   */
  public long getFileHandle() {
    return bb.getLong(0x18);
  }

  /**
   * Set File handle. May be filled in by filesystem in open(). Available in all other file
   * operations
   */
  public FileInfo putFileHandle(final long value) {
    bb.putLong(0x18, value);
    return this;
  }

  /**
   * Get Lock owner id. Available in locking operations and flush
   */
  public long getLockOwner() {
    return bb.getLong(0x20);
  }

  /**
   * Set Lock owner id. Available in locking operations and flush
   */
  public FileInfo putLockOwner(final long value) {
    bb.putLong(0x20, value);
    return this;
  }
}
