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

package alluxio.jnifuse.struct;

import alluxio.util.OSUtils;

import java.nio.ByteBuffer;

public class FileStat extends Struct {
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

  public static boolean S_ISTYPE(int mode, int mask) {
    return (mode & S_IFMT) == mask;
  }

  public static boolean S_ISDIR(int mode) {
    return S_ISTYPE(mode, S_IFDIR);
  }

  public static boolean S_ISCHR(int mode) {
    return S_ISTYPE(mode, S_IFCHR);
  }

  public static boolean S_ISBLK(int mode) {
    return S_ISTYPE(mode, S_IFBLK);
  }

  public static boolean S_ISREG(int mode) {
    return S_ISTYPE(mode, S_IFREG);
  }

  public static boolean S_ISFIFO(int mode) {
    return S_ISTYPE(mode, S_IFIFO);
  }

  public static boolean S_ISLNK(int mode) {
    return S_ISTYPE(mode, S_IFLNK);
  }

  public FileStat(ByteBuffer buffer) {
    super(buffer);
    if (OSUtils.isMacOS()) {
      st_dev = new Unsigned64();
      st_mode = new Unsigned32();
      st_nlink = new Unsigned64();
      st_ino = new Unsigned64();
      st_uid = new Unsigned32();
      st_gid = new Unsigned32();
      st_rdev = new Unsigned64();
      st_atim = new Timespec();
      st_mtim = new Timespec();
      st_ctim = new Timespec();
      st_birthtime = new Timespec();
      st_size = new SignedLong();
      st_blocks = new SignedLong();
      st_blksize = new SignedLong();
      st_flags = new Unsigned32();
      st_gen = new Unsigned32();
      new Signed32();
      new Signed64();
      new Signed64();

      pad1 = null;
    } else {
      // Linux platform
      st_dev = new Unsigned64();
      pad1 = null;
      st_ino = new Unsigned64();
      st_nlink = new Unsigned64();
      st_mode = new Unsigned32();
      st_uid = new Unsigned32();
      st_gid = new Unsigned32();
      st_rdev = new Unsigned64();
      st_size = new SignedLong();
      st_blksize = new SignedLong();
      st_blocks = new SignedLong();
      st_atim = new Timespec();
      st_mtim = new Timespec();
      st_ctim = new Timespec();

      st_birthtime = null;
      st_flags = null;
      st_gen = null;
    }
  }

  public final Unsigned64 st_dev;
  public final Unsigned16 pad1;
  public final Unsigned64 st_ino;
  public final Unsigned64 st_nlink;
  public final Unsigned32 st_mode;
  public final Unsigned32 st_uid;
  public final Unsigned32 st_gid;
  public final Unsigned64 st_rdev;
  public final SignedLong st_size;
  public final SignedLong st_blksize;
  public final SignedLong st_blocks;
  public final Timespec st_atim;
  public final Timespec st_mtim;
  public final Timespec st_ctim;
  public final Timespec st_birthtime;

  /** MacOS specific */
  public final Unsigned32 st_flags;
  public final Unsigned32 st_gen;

  public static FileStat wrap(ByteBuffer buffer) {
    return new FileStat(buffer);
  }
}
