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

package alluxio.fuse;


import jnr.constants.platform.OpenFlags;

public final class AlluxioFuseOpenUtils {
  // TODO(lu) change to bit operation to speed up
  // FUSE open flags accordance to open flags on Linux:
  
  // r Open file for reading. An exception occurs if the file does not exist.
  // (Decimal): 32768 (Octal): 00000100000 (Hexadecimal): 00008000
  // (Binary ): 00000000000000001000000000000000
  private static final int READ =  32768;
  // r+/w+ Open file for reading and writing. An exception occurs if the file
  // does not exist. If open for writing, the file is created (if it does not
  // exist) or truncated (if it exists).
  // (Decimal): 32770 (Octal): 00000100002 (Hexadecimal): 00008002
  // (Binary ): 00000000000000001000000000000010
  private static final int R_OR_W_PLUS =  32770;
  // rs Open file for reading in synchronous mode. Instructs the operating
  // system to bypass the local file system cache. This is primarily useful
  // for opening files on NFS mounts as it allows you to skip the
  // potentially stale local cache. It has a very real impact on I/O 
  // performance so don't use this flag unless you need it. Note that this
  // doesn't turn fs.open() into a synchronous blocking call. If that's
  // what you want then you should be using fs.openSync().
  // (Decimal): 36864 (Octal): 00000110000 (Hexadecimal): 00009000
  // (Binary ): 00000000000000001001000000000000
  private static final int READ_SYNC =  36864;
  // rs+ Open file for reading and writing, telling the OS to open it
  // synchronously. See notes for 'rs' about using this with caution.
  //(Decimal): 36866 (Octal): 00000110002 (Hexadecimal): 00009002
  // (Binary ): 00000000000000001001000000000010
  private static final int RS_PLUS =  36866;
  // w Open file for writing. The file is created (if it does not exist) or
  // truncated (if it exists).
  // (Decimal): 32769 (Octal): 00000100001 (Hexadecimal): 00008001
  // (Binary ): 00000000000000001000000000000001
  private static final int WRITE =  32769;
  // wx  FUSE.open() is never called
  // a Open file for appending. The file is created if it does not exist.
  // (Decimal): 33793 (Octal): 00000102001 (Hexadecimal): 00008401
  // (Binary ): 00000000000000001000010000000001
  private static final int APPEND = 33793;
  // ax  FUSE.open() is never called
  // a+  # Open file for reading and appending. The file is created if it
  // does not exist.
  // (Decimal): 33794 (Octal): 00000102002 (Hexadecimal): 00008402
  // (Binary ): 00000000000000001000010000000010
  private static final int APPEND_PLUS = 33794;
  // ax+ FUSE.open() is never called

  public static OpenType getOpenType(int flag) {
    OpenFlags openFlags = OpenFlags.valueOf(flag);
    switch (openFlags) {
      case O_RDONLY:
      case O_NONBLOCK:
      case O_EVTONLY:
        return OpenType.READ_ONLY;
      case O_WRONLY:
      // If file exists, error out with EEXIST
      case O_CREAT:
      // Truncate to length 0
      case O_TRUNC:
      // If file exists, error out with EEXIST
      case O_EXCL: 
      case O_SYNC:
        return OpenType.WRITE_ONLY;
      case O_RDWR:
        return OpenType.READ_WRITE;
      case O_APPEND:
      // Use readdir() to read directory
      case O_DIRECTORY:
        return OpenType.NOT_SUPPORTED;
      default:
        return getSpecialOpenType(flag);
    }
  }
  
  private static OpenType getSpecialOpenType(int flag) {
    switch (flag) {
      case READ:
      case READ_SYNC:
        return OpenType.READ_ONLY;
      case R_OR_W_PLUS:
      case RS_PLUS:
        return OpenType.READ_WRITE;
      case WRITE:
        return OpenType.WRITE_ONLY;
      case APPEND:
      case APPEND_PLUS:
        return OpenType.NOT_SUPPORTED;
      default:
        return OpenType.UNKNOWN;
    }
  }

  public enum OpenType {
    READ_ONLY,
    WRITE_ONLY,
    // TODO(maobaolong): Add an option to decide whether reject rw flag
    // Alluxio does not support open a file for reading and writing concurrently.
    // Read or write behavior is decided in the first read() or write()
    // if read then write or write then read, we error out
    READ_WRITE,
    NOT_SUPPORTED,
    UNKNOWN,
    ;
  }
}
