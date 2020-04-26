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

public class FileStat {
  public static final int S_IFIFO = 0010000; // named pipe (fifo)
  public static final int S_IFCHR = 0020000; // character special
  public static final int S_IFDIR = 0040000; // directory
  public static final int S_IFBLK = 0060000; // block special
  public static final int S_IFREG = 0100000; // regular
  public static final int S_IFLNK = 0120000; // symbolic link
  public static final int S_IFSOCK = 0140000; // socket
  public static final int S_IFMT = 0170000; // file mask for type checks
  public static final int S_ISUID = 0004000; // set user id on execution
  public static final int S_ISGID = 0002000; // set group id on execution
  public static final int S_ISVTX = 0001000; // save swapped text even after use
  public static final int S_IRUSR = 0000400; // read permission, owner
  public static final int S_IWUSR = 0000200; // write permission, owner
  public static final int S_IXUSR = 0000100; // execute/search permission, owner
  public static final int S_IRGRP = 0000040; // read permission, group
  public static final int S_IWGRP = 0000020; // write permission, group
  public static final int S_IXGRP = 0000010; // execute/search permission, group
  public static final int S_IROTH = 0000004; // read permission, other
  public static final int S_IWOTH = 0000002; // write permission, other
  public static final int S_IXOTH = 0000001; // execute permission, other

  public static final int ALL_READ = S_IRUSR | S_IRGRP | S_IROTH;
  public static final int ALL_WRITE = S_IWUSR | S_IWGRP | S_IWOTH;
  public static final int S_IXUGO = S_IXUSR | S_IXGRP | S_IXOTH;

  final ByteBuffer bb;

  public FileStat(ByteBuffer bb) {
    this.bb = bb;
    this.bb.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Get ID of device containing file
   */
  public long getDev() {
    return bb.getLong(0x0);
  }

  /**
   * Set ID of device containing file
   */
  public FileStat putDev(final long value) {
    bb.putLong(0x0, value);
    return this;
  }

  /**
   * Get inode number
   */
  public long getInode() {
    return bb.getLong(0x8);
  }

  /**
   * Set inode number
   */
  public FileStat putInode(final long value) {
    bb.putLong(0x8, value);
    return this;
  }

  /**
   * Get protection
   */
  public int getMode() {
    return bb.getInt(0x18);
  }

  /**
   * Set protection
   */
  public FileStat putMode(final int value) {
    bb.putInt(0x18, value);
    return this;
  }

  /**
   * Get number of hard links
   */
  public long getLinkCount() {
    return bb.getLong(0x10);
  }

  /**
   * Set number of hard links
   */
  public FileStat putLinkCount(final long value) {
    bb.putLong(0x10, value);
    return this;
  }

  /**
   * Get user ID of owner
   */
  public int getUserId() {
    return bb.getInt(0x1c);
  }

  /**
   * Set user ID of owner
   */
  public FileStat putUserId(final int value) {
    bb.putInt(0x1c, value);
    return this;
  }

  /**
   * Get group ID of owner
   */
  public int getGroupId() {
    return bb.getInt(0x20);
  }

  /**
   * Set group ID of owner
   */
  public FileStat putGroupId(final int value) {
    bb.putInt(0x20, value);
    return this;
  }

  /**
   * Get device ID (if special file)
   */
  public long getRDev() {
    return bb.getLong(0x28);
  }

  /**
   * Set device ID (if special file)
   */
  public FileStat putRDev(final long value) {
    bb.putLong(0x28, value);
    return this;
  }

  /**
   * Get total size, in bytes
   */
  public long getSize() {
    return bb.getLong(0x30);
  }

  /**
   * Set total size, in bytes
   */
  public FileStat putSize(final long value) {
    bb.putLong(0x30, value);
    return this;
  }

  /**
   * Get blocksize for file system I/O
   */
  public long getBlkSize() {
    return bb.getLong(0x38);
  }

  /**
   * Set blocksize for file system I/O
   */
  public FileStat putBlkSize(final long value) {
    bb.putLong(0x38, value);
    return this;
  }

  /**
   * Get number of 512B blocks allocated
   */
  public long getBlocks() {
    return bb.getLong(0x40);
  }

  /**
   * Set number of 512B blocks allocated
   */
  public FileStat putBlocks(final long value) {
    bb.putLong(0x40, value);
    return this;
  }

  /**
   * Get time of last access
   */
  public long getAccessTime() {
    return bb.getLong(0x48);
  }

  /**
   * Set time of last access
   */
  public FileStat putAccessTime(final long value) {
    bb.putLong(0x48, value);
    return this;
  }

  /**
   * Get time of last modification
   */
  public long getModTime() {
    return bb.getLong(0x58);
  }

  /**
   * Set time of last modification
   */
  public FileStat putModTime(final long value) {
    bb.putLong(0x58, value);
    return this;
  }

  /**
   * Get time of last status change
   */
  public long getCTime() {
    return bb.getLong(0x68);
  }

  /**
   * Set time of last status change
   */
  public FileStat putCTime(final long value) {
    bb.putLong(0x68, value);
    return this;
  }

  /**
   * st_dev: int 4 pad: ... st_ino: unsigned long
   *
   * st_nlink: int st_mode: int
   *
   * st_uid: int st_gid: int st_rdev:int pad2: ... st_size: long st_blksize: int st_blocks: int
   * st_atim:
   *
   */

}
