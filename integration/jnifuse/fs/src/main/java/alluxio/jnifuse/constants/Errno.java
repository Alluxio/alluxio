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

package alluxio.jnifuse.constants;

public enum Errno {
  EPERM(1),
  ENOENT(2),
  ESRCH(3),
  EINTR(4),
  EIO(5),
  ENXIO(6),
  E2BIG(7),
  ENOEXEC(8),
  EBADF(9),
  ECHILD(10),
  EDEADLK(35),
  ENOMEM(12),
  EACCES(13),
  EFAULT(14),
  ENOTBLK(15),
  EBUSY(16),
  EEXIST(17),
  EXDEV(18),
  ENODEV(19),
  ENOTDIR(20),
  EISDIR(21),
  EINVAL(22),
  ENFILE(23),
  EMFILE(24),
  ENOTTY(25),
  ETXTBSY(26),
  EFBIG(27),
  ENOSPC(28),
  ESPIPE(29),
  EROFS(30),
  EMLINK(31),
  EPIPE(32),
  EDOM(33),
  ERANGE(34),
  EWOULDBLOCK(11),
  EAGAIN(11),
  EINPROGRESS(115),
  EALREADY(114),
  ENOTSOCK(88),
  EDESTADDRREQ(89),
  EMSGSIZE(90),
  EPROTOTYPE(91),
  ENOPROTOOPT(92),
  EPROTONOSUPPORT(93),
  ESOCKTNOSUPPORT(94),
  EOPNOTSUPP(95),
  EPFNOSUPPORT(96),
  EAFNOSUPPORT(97),
  EADDRINUSE(98),
  EADDRNOTAVAIL(99),
  ENETDOWN(100),
  ENETUNREACH(101),
  ENETRESET(102),
  ECONNABORTED(103),
  ECONNRESET(104),
  ENOBUFS(105),
  EISCONN(106),
  ENOTCONN(107),
  ESHUTDOWN(108),
  ETOOMANYREFS(109),
  ETIMEDOUT(110),
  ECONNREFUSED(111),
  ELOOP(40),
  ENAMETOOLONG(36),
  EHOSTDOWN(112),
  EHOSTUNREACH(113),
  ENOTEMPTY(39),
  EUSERS(87),
  EDQUOT(122),
  ESTALE(116),
  EREMOTE(66),
  ENOLCK(37),
  ENOSYS(38),
  EOVERFLOW(75),
  EIDRM(43),
  ENOMSG(42),
  EILSEQ(84),
  EBADMSG(74),
  EMULTIHOP(72),
  ENODATA(61),
  ENOLINK(67),
  ENOSR(63),
  ENOSTR(60),
  EPROTO(71),
  ETIME(62);
  private final int value;
  private Errno(int value) { this.value = value; }
  public static final long MIN_VALUE = 1;
  public static final long MAX_VALUE = 122;

  static final class StringTable {
    public static final java.util.Map<Errno, String> descriptions = generateTable();
    public static java.util.Map<Errno, String> generateTable() {
      java.util.Map<Errno, String> map = new java.util.EnumMap<Errno, String>(Errno.class);
      map.put(EPERM, "Operation not permitted");
      map.put(ENOENT, "No such file or directory");
      map.put(ESRCH, "No such process");
      map.put(EINTR, "Interrupted system call");
      map.put(EIO, "Input/output error");
      map.put(ENXIO, "No such device or address");
      map.put(E2BIG, "Argument list too long");
      map.put(ENOEXEC, "Exec format error");
      map.put(EBADF, "Bad file descriptor");
      map.put(ECHILD, "No child processes");
      map.put(EDEADLK, "Resource deadlock avoided");
      map.put(ENOMEM, "Cannot allocate memory");
      map.put(EACCES, "Permission denied");
      map.put(EFAULT, "Bad address");
      map.put(ENOTBLK, "Block device required");
      map.put(EBUSY, "Device or resource busy");
      map.put(EEXIST, "File exists");
      map.put(EXDEV, "Invalid cross-device link");
      map.put(ENODEV, "No such device");
      map.put(ENOTDIR, "Not a directory");
      map.put(EISDIR, "Is a directory");
      map.put(EINVAL, "Invalid argument");
      map.put(ENFILE, "Too many open files in system");
      map.put(EMFILE, "Too many open files");
      map.put(ENOTTY, "Inappropriate ioctl for device");
      map.put(ETXTBSY, "Text file busy");
      map.put(EFBIG, "File too large");
      map.put(ENOSPC, "No space left on device");
      map.put(ESPIPE, "Illegal seek");
      map.put(EROFS, "Read-only file system");
      map.put(EMLINK, "Too many links");
      map.put(EPIPE, "Broken pipe");
      map.put(EDOM, "Numerical argument out of domain");
      map.put(ERANGE, "Numerical result out of range");
      map.put(EWOULDBLOCK, "Resource temporarily unavailable");
      map.put(EAGAIN, "Resource temporarily unavailable");
      map.put(EINPROGRESS, "Operation now in progress");
      map.put(EALREADY, "Operation already in progress");
      map.put(ENOTSOCK, "Socket operation on non-socket");
      map.put(EDESTADDRREQ, "Destination address required");
      map.put(EMSGSIZE, "Message too long");
      map.put(EPROTOTYPE, "Protocol wrong type for socket");
      map.put(ENOPROTOOPT, "Protocol not available");
      map.put(EPROTONOSUPPORT, "Protocol not supported");
      map.put(ESOCKTNOSUPPORT, "Socket type not supported");
      map.put(EOPNOTSUPP, "Operation not supported");
      map.put(EPFNOSUPPORT, "Protocol family not supported");
      map.put(EAFNOSUPPORT, "Address family not supported by protocol");
      map.put(EADDRINUSE, "Address already in use");
      map.put(EADDRNOTAVAIL, "Cannot assign requested address");
      map.put(ENETDOWN, "Network is down");
      map.put(ENETUNREACH, "Network is unreachable");
      map.put(ENETRESET, "Network dropped connection on reset");
      map.put(ECONNABORTED, "Software caused connection abort");
      map.put(ECONNRESET, "Connection reset by peer");
      map.put(ENOBUFS, "No buffer space available");
      map.put(EISCONN, "Transport endpoint is already connected");
      map.put(ENOTCONN, "Transport endpoint is not connected");
      map.put(ESHUTDOWN, "Cannot send after transport endpoint shutdown");
      map.put(ETOOMANYREFS, "Too many references: cannot splice");
      map.put(ETIMEDOUT, "Connection timed out");
      map.put(ECONNREFUSED, "Connection refused");
      map.put(ELOOP, "Too many levels of symbolic links");
      map.put(ENAMETOOLONG, "File name too long");
      map.put(EHOSTDOWN, "Host is down");
      map.put(EHOSTUNREACH, "No route to host");
      map.put(ENOTEMPTY, "Directory not empty");
      map.put(EUSERS, "Too many users");
      map.put(EDQUOT, "Disk quota exceeded");
      map.put(ESTALE, "Stale NFS file handle");
      map.put(EREMOTE, "Object is remote");
      map.put(ENOLCK, "No locks available");
      map.put(ENOSYS, "Function not implemented");
      map.put(EOVERFLOW, "Value too large for defined data type");
      map.put(EIDRM, "Identifier removed");
      map.put(ENOMSG, "No message of desired type");
      map.put(EILSEQ, "Invalid or incomplete multibyte or wide character");
      map.put(EBADMSG, "Bad message");
      map.put(EMULTIHOP, "Multihop attempted");
      map.put(ENODATA, "No data available");
      map.put(ENOLINK, "Link has been severed");
      map.put(ENOSR, "Out of streams resources");
      map.put(ENOSTR, "Device not a stream");
      map.put(EPROTO, "Protocol error");
      map.put(ETIME, "Timer expired");
      return map;
    }
  }
  public final String toString() { return Errno.StringTable.descriptions.get(this); }
  public final int value() { return value; }
  public final int intValue() { return value; }
  public final long longValue() { return value; }
  public final boolean defined() { return true; }
}

