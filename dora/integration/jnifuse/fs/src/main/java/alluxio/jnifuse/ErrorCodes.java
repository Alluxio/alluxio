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

package alluxio.jnifuse;

import alluxio.jnifuse.constants.Errno;

public class ErrorCodes {
  /**
   * Argument list too long
   */
  public static int E2BIG() {
    return Errno.E2BIG.intValue();
  }

  /**
   * Permission denied
   */
  public static int EACCES() {
    return Errno.EACCES.intValue();
  }

  /**
   * Address already in use
   */
  public static int EADDRINUSE() {
    return Errno.EADDRINUSE.intValue();
  }

  /**
   * Cannot assign requested address
   */
  public static int EADDRNOTAVAIL() {
    return Errno.EADDRNOTAVAIL.intValue();
  }

  /**
   * Advertise error
   */
  public static int EADV() {
    return 68;
  }

  /**
   * Address family not supported by protocol
   */
  public static int EAFNOSUPPORT() {
    return Errno.EAFNOSUPPORT.intValue();
  }

  /**
   * Try again
   */
  public static int EAGAIN() {
    return Errno.EAGAIN.intValue();
  }

  /**
   * Operation already in progress
   */
  public static int EALREADY() {
    return Errno.EALREADY.intValue();
  }

  /**
   * Invalid exchange
   */
  public static int EBADE() {
    return 52;
  }

  /**
   * Bad file number
   */
  public static int EBADF() {
    return Errno.EBADF.intValue();
  }

  /**
   * File descriptor in bad state
   */
  public static int EBADFD() {
    return 77;
  }

  /**
   * Not a data message
   */
  public static int EBADMSG() {
    return Errno.EBADMSG.intValue();
  }

  /**
   * Invalid request descriptor
   */
  public static int EBADR() {
    return 53;
  }

  /**
   * Invalid request code
   */
  public static int EBADRQC() {
    return 56;
  }

  /**
   * Invalid slot
   */
  public static int EBADSLT() {
    return 57;
  }

  /**
   * Bad font file format
   */
  public static int EBFONT() {
    return 59;
  }

  /**
   * Device or resource busy
   */
  public static int EBUSY() {
    return Errno.EBUSY.intValue();
  }

  /**
   * Operation Canceled
   */
  public static int ECANCELED() {
    return 125;
  }

  /**
   * No child processes
   */
  public static int ECHILD() {
    return Errno.ECHILD.intValue();
  }

  /**
   * Channel number out of range
   */
  public static int ECHRNG() {
    return 44;
  }

  /**
   * Communication error on send
   */
  public static int ECOMM() {
    return 70;
  }

  /**
   * Software caused connection abort
   */
  public static int ECONNABORTED() {
    return Errno.ECONNABORTED.intValue();
  }

  /**
   * Connection refused
   */
  public static int ECONNREFUSED() {
    return Errno.ECONNREFUSED.intValue();
  }

  /**
   * Connection reset by peer
   */
  public static int ECONNRESET() {
    return Errno.ECONNRESET.intValue();
  }

  /**
   * Resource deadlock would occur
   */
  public static int EDEADLK() {
    return Errno.EDEADLK.intValue();
  }

  public static int EDEADLOCK() {
    return EDEADLK();
  }

  /**
   * Destination address required
   */
  public static int EDESTADDRREQ() {
    return Errno.EDESTADDRREQ.intValue();
  }

  /**
   * Math argument out of domain of func
   */
  public static int EDOM() {
    return Errno.EDOM.intValue();
  }

  /**
   * RFS specific error
   */
  public static int EDOTDOT() {
    return 73;
  }

  /**
   * Quota exceeded
   */
  public static int EDQUOT() {
    return Errno.EDQUOT.intValue();
  }

  /**
   * File exists
   */
  public static int EEXIST() {
    return Errno.EEXIST.intValue();
  }

  /**
   * Bad address
   */
  public static int EFAULT() {
    return Errno.EFAULT.intValue();
  }

  /**
   * File too large
   */
  public static int EFBIG() {
    return Errno.EFBIG.intValue();
  }

  /**
   * Host is down
   */
  public static int EHOSTDOWN() {
    return Errno.EHOSTDOWN.intValue();
  }

  /**
   * No route to host
   */
  public static int EHOSTUNREACH() {
    return Errno.EHOSTUNREACH.intValue();
  }

  /**
   * Identifier removed
   */
  public static int EIDRM() {
    return Errno.EIDRM.intValue();
  }

  /**
   * Illegal byte sequence
   */
  public static int EILSEQ() {
    return Errno.EILSEQ.intValue();
  }

  /**
   * Operation now in progress
   */
  public static int EINPROGRESS() {
    return Errno.EINPROGRESS.intValue();
  }

  /**
   * Interrupted system call
   */
  public static int EINTR() {
    return Errno.EINTR.intValue();
  }

  /**
   * Invalid argument
   */
  public static int EINVAL() {
    return Errno.EINVAL.intValue();
  }

  /**
   * I/O error
   */
  public static int EIO() {
    return Errno.EIO.intValue();
  }

  /**
   * Transport endpoint is already connected
   */
  public static int EISCONN() {
    return Errno.EISCONN.intValue();
  }

  /**
   * Is a directory
   */
  public static int EISDIR() {
    return Errno.EISDIR.intValue();
  }

  /**
   * Is a named type file
   */
  public static int EISNAM() {
    return 120;
  }

  /**
   * Key has expired
   */
  public static int EKEYEXPIRED() {
    return 127;
  }

  /**
   * Key was rejected by service
   */
  public static int EKEYREJECTED() {
    return 129;
  }

  /**
   * Key has been revoked
   */
  public static int EKEYREVOKED() {
    return 128;
  }

  /**
   * Level 2 halted
   */
  public static int EL2HLT() {
    return 51;
  }

  /**
   * Level 2 not synchronized
   */
  public static int EL2NSYNC() {
    return 45;
  }

  /**
   * Level 3 halted
   */
  public static int EL3HLT() {
    return 46;
  }

  /**
   * Level 3 reset
   */
  public static int EL3RST() {
    return 47;
  }

  /**
   * Can not access a needed shared library
   */
  public static int ELIBACC() {
    return 79;
  }

  /**
   * Accessing a corrupted shared library
   */
  public static int ELIBBAD() {
    return 80;
  }

  /**
   * Cannot exec a shared library directly
   */
  public static int ELIBEXEC() {
    return 83;
  }

  /**
   * Attempting to link in too many shared libraries
   */
  public static int ELIBMAX() {
    return 82;
  }

  /**
   * .lib section in a.out corrupted
   */
  public static int ELIBSCN() {
    return 81;
  }

  /**
   * Link number out of range
   */
  public static int ELNRNG() {
    return 48;
  }

  /**
   * Too many symbolic links encountered
   */
  public static int ELOOP() {
    return Errno.ELOOP.intValue();
  }

  /**
   * Wrong medium type
   */
  public static int EMEDIUMTYPE() {
    return 124;
  }

  /**
   * Too many open files
   */
  public static int EMFILE() {
    return Errno.EMFILE.intValue();
  }

  /**
   * Too many links
   */
  public static int EMLINK() {
    return Errno.EMLINK.intValue();
  }

  /**
   * Message too long
   */
  public static int EMSGSIZE() {
    return Errno.EMSGSIZE.intValue();
  }

  /**
   * Multihop attempted
   */
  public static int EMULTIHOP() {
    return Errno.EMULTIHOP.intValue();
  }

  /**
   * File name too long
   */
  public static int ENAMETOOLONG() {
    return Errno.ENAMETOOLONG.intValue();
  }

  /**
   * No XENIX semaphores available
   */
  public static int ENAVAIL() {
    return 119;
  }

  /**
   * Network is down
   */
  public static int ENETDOWN() {
    return Errno.ENETDOWN.intValue();
  }

  /**
   * Network dropped connection because of reset
   */
  public static int ENETRESET() {
    return Errno.ENETRESET.intValue();
  }

  /**
   * Network is unreachable
   */
  public static int ENETUNREACH() {
    return Errno.ENETUNREACH.intValue();
  }

  /**
   * File table overflow
   */
  public static int ENFILE() {
    return Errno.ENFILE.intValue();
  }

  /**
   * No anode
   */
  public static int ENOANO() {
    return 55;
  }

  /**
   * No buffer space available
   */
  public static int ENOBUFS() {
    return Errno.ENOBUFS.intValue();
  }

  /**
   * No CSI structure available
   */
  public static int ENOCSI() {
    return 50;
  }

  /**
   * No data available
   */
  public static int ENODATA() {
    return Errno.ENODATA.intValue();
  }

  /**
   * No such device
   */
  public static int ENODEV() {
    return Errno.ENODEV.intValue();
  }

  /**
   * No such file or directory
   */
  public static int ENOENT() {
    return Errno.ENOENT.intValue();
  }

  /**
   * Exec format error
   */
  public static int ENOEXEC() {
    return Errno.ENOEXEC.intValue();
  }

  /**
   * Required key not available
   */
  public static int ENOKEY() {
    return 126;
  }

  /**
   * No record locks available
   */
  public static int ENOLCK() {
    return Errno.ENOLCK.intValue();
  }

  /**
   * Link has been severed
   */
  public static int ENOLINK() {
    return Errno.ENOLINK.intValue();
  }

  /**
   * No medium found
   */
  public static int ENOMEDIUM() {
    return 123;
  }

  /**
   * Out of memory
   */
  public static int ENOMEM() {
    return Errno.ENOMEM.intValue();
  }

  /**
   * No message of desired type
   */
  public static int ENOMSG() {
    return Errno.ENOMSG.intValue();
  }

  /**
   * Machine is not on the network
   */
  public static int ENONET() {
    return 64;
  }

  /**
   * Package not installed
   */
  public static int ENOPKG() {
    return 65;
  }

  /**
   * Protocol not available
   */
  public static int ENOPROTOOPT() {
    return Errno.ENOPROTOOPT.intValue();
  }

  /**
   * No space left on device
   */
  public static int ENOSPC() {
    return Errno.ENOSPC.intValue();
  }

  /**
   * Out of streams resources
   */
  public static int ENOSR() {
    return Errno.ENOSR.intValue();
  }

  /**
   * Device not a stream
   */
  public static int ENOSTR() {
    return Errno.ENOSTR.intValue();
  }

  /**
   * Function not implemented
   */
  public static int ENOSYS() {
    return Errno.ENOSYS.intValue();
  }

  /**
   * Block device required
   */
  public static int ENOTBLK() {
    return Errno.ENOTBLK.intValue();
  }

  /**
   * Transport endpoint is not connected
   */
  public static int ENOTCONN() {
    return Errno.ENOTCONN.intValue();
  }

  /**
   * Not a directory
   */
  public static int ENOTDIR() {
    return Errno.ENOTDIR.intValue();
  }

  /**
   * Directory not empty
   */
  public static int ENOTEMPTY() {
    return Errno.ENOTEMPTY.intValue();
  }

  /**
   * Not a XENIX named type file
   */
  public static int ENOTNAM() {
    return 118;
  }

  /**
   * State not recoverable
   */
  public static int ENOTRECOVERABLE() {
    return 131;
  }

  /**
   * Socket operation on non-socket
   */
  public static int ENOTSOCK() {
    return Errno.ENOTSOCK.intValue();
  }

  /**
   * Not a typewriter
   */
  public static int ENOTTY() {
    return Errno.ENOTTY.intValue();
  }

  /**
   * Name not unique on network
   */
  public static int ENOTUNIQ() {
    return 76;
  }

  /**
   * No such device or address
   */
  public static int ENXIO() {
    return Errno.ENXIO.intValue();
  }

  /**
   * Operation not supported on transport endpoint
   */
  public static int EOPNOTSUPP() {
    return Errno.EOPNOTSUPP.intValue();
  }

  /**
   * Value too large for defined data type
   */
  public static int EOVERFLOW() {
    return Errno.EOVERFLOW.intValue();
  }

  /**
   * Owner died
   */
  public static int EOWNERDEAD() {
    return 130;
  }

  /**
   * Operation not permitted
   */
  /**
   * Operation not permitted
   */
  public static int EPERM() {
    return Errno.EPERM.intValue();
  }

  /**
   * Protocol family not supported
   */
  public static int EPFNOSUPPORT() {
    return Errno.EPFNOSUPPORT.intValue();
  }

  /**
   * Broken pipe
   */
  public static int EPIPE() {
    return Errno.EPIPE.intValue();
  }

  /**
   * Protocol error
   */
  public static int EPROTO() {
    return Errno.EPROTO.intValue();
  }

  /**
   * Protocol not supported
   */
  public static int EPROTONOSUPPORT() {
    return Errno.EPROTONOSUPPORT.intValue();
  }

  /**
   * Protocol wrong type for socket
   */
  public static int EPROTOTYPE() {
    return Errno.EPROTOTYPE.intValue();
  }

  /**
   * Math result not representable
   */
  public static int ERANGE() {
    return Errno.ERANGE.intValue();
  }

  /**
   * Remote address changed
   */
  public static int EREMCHG() {
    return 78;
  }

  /**
   * Object is remote
   */
  public static int EREMOTE() {
    return Errno.EREMOTE.intValue();
  }

  /**
   * Remote I/O error
   */
  public static int EREMOTEIO() {
    return 121;
  }

  /**
   * Interrupted system call should be restarted
   */
  public static int ERESTART() {
    return 85;
  }

  /**
   * Read-only file system
   */
  public static int EROFS() {
    return Errno.EROFS.intValue();
  }

  /**
   * Cannot send after transport endpoint shutdown
   */
  public static int ESHUTDOWN() {
    return Errno.ESHUTDOWN.intValue();
  }

  /**
   * Socket type not supported
   */
  public static int ESOCKTNOSUPPORT() {
    return Errno.ESOCKTNOSUPPORT.intValue();
  }

  /**
   * Illegal seek
   */
  public static int ESPIPE() {
    return Errno.ESPIPE.intValue();
  }

  /**
   * No such process
   */
  public static int ESRCH() {
    return Errno.ESRCH.intValue();
  }

  /**
   * Srmount error
   */
  public static int ESRMNT() {
    return 69;
  }

  /**
   * Stale file handle
   */
  public static int ESTALE() {
    return Errno.ESTALE.intValue();
  }

  /**
   * Streams pipe error
   */
  public static int ESTRPIPE() {
    return 86;
  }

  /**
   * Timer expired
   */
  public static int ETIME() {
    return Errno.ETIME.intValue();
  }

  /**
   * Connection timed out
   */
  public static int ETIMEDOUT() {
    return Errno.ETIMEDOUT.intValue();
  }

  /**
   * Too many references: cannot splice
   */
  public static int ETOOMANYREFS() {
    return Errno.ETOOMANYREFS.intValue();
  }

  /**
   * Text file busy
   */
  public static int ETXTBSY() {
    return Errno.ETXTBSY.intValue();
  }

  /**
   * Structure needs cleaning
   */
  public static int EUCLEAN() {
    return 117;
  }

  /**
   * Protocol driver not attached
   */
  public static int EUNATCH() {
    return 49;
  }

  /**
   * Too many users
   */
  public static int EUSERS() {
    return Errno.EUSERS.intValue();
  }

  /**
   * Operation would block
   */
  public static int EWOULDBLOCK() {
    return Errno.EWOULDBLOCK.intValue();
  }

  /**
   * Cross-device link
   */
  public static int EXDEV() {
    return Errno.EXDEV.intValue();
  }

  /**
   * Exchange full
   */
  public static int EXFULL() {
    return 54;
  }

  /**
   * The extended attribute does not exist
   */
  public static int ENOATTR() {
    return 93;
  }

  /**
   * The file system does not support extended attributes or has the feature disabled
   */
  public static int ENOTSUP() {
    return 45;
  }

  private ErrorCodes() {
  }
}
