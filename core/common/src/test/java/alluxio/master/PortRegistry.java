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

package alluxio.master;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for reserving ports during tests.
 *
 * The registry reserves ports by taking file locks on files in the port coordination directory.
 * This doesn't prevent external processes from stealing our ports, but it will prevent us from
 * conflicting with ourselves. We can then run tests in a dockerized environment to completely
 * prevent conflicts.
 *
 * The default coordination directory is determined by the "user.dir" jvm property. The coordination
 * directory can be overridden by setting the ALLUXIO_PORT_COORDINATION_DIR environment variable.
 */
public final class PortRegistry {
  private static final String PORT_COORDINATION_DIR_PROPERTY = "ALLUXIO_PORT_COORDINATION_DIR";

  @VisibleForTesting
  static final Registry INSTANCE = new Registry();

  private PortRegistry() {} // Class should not be instatiated.

  /**
   * Reserves a free port so that other tests will not take it.
   *
   * @return the free port
   */
  public static int reservePort() {
    return INSTANCE.reservePort();
  }

  /**
   * @param port the port to release
   */
  public static void release(int port) {
    INSTANCE.release(port);
  }

  /**
   * Clears the registry.
   */
  public static void clear() {
    INSTANCE.clear();
  }

  /**
   * @return a port that is currently free. This does not reserve the port, so the port may be taken
   *         by the time this method returns.
   */
  public static int getFreePort() {
    int port;
    try {
      ServerSocket socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return port;
  }

  @VisibleForTesting
  static class Registry {
    // Map from port number to the reservation for that port.
    private final Map<Integer, Reservation> mReserved = new ConcurrentHashMap<>();
    private final File mCoordinationDir;

    public Registry() {
      String dir = System.getenv(PORT_COORDINATION_DIR_PROPERTY);
      if (dir == null) {
        dir = System.getProperty("user.dir");
      }
      mCoordinationDir = new File(dir, ".port_coordination");
      mCoordinationDir.mkdirs();
    }

    /**
     * Reserves a free port so that other tests will not take it.
     *
     * @return the free port
     */
    public int reservePort() {
      for (int i = 0; i < 1000; i++) {
        int port = getFreePort();
        if (lockPort(port)) {
          return port;
        }
      }
      throw new RuntimeException("Failed to acquire port");
    }

    /**
     * Attempts to lock the given port.
     *
     * @param port the port to lock
     * @return whether the locking succeeded
     */
    public boolean lockPort(int port) {
      File portFile = portFile(port);
      try {
        FileChannel channel = new RandomAccessFile(portFile, "rw").getChannel();
        FileLock lock = channel.tryLock();
        if (lock == null) {
          channel.close();
          return false;
        }
        mReserved.put(port, new Reservation(portFile, lock));
        return true;
      } catch (IOException | OverlappingFileLockException e) {
        return false;
      }
    }

    /**
     * @param port the port to release
     */
    public void release(int port) {
      Reservation r = mReserved.remove(port);
      if (r != null) {
        // If delete fails, we may leave a file behind. However, the file will be unlocked, so
        // another process can still take the port.
        r.mFile.delete();
        try {
          r.mLock.release();
          r.mLock.channel().close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Clears the registry.
     */
    public void clear() {
      new HashSet<>(mReserved.keySet()).forEach(this::release);
    }

    /**
     * Creates a file in coordination dir to lock the port.
     *
     * @param port the port to lock
     * @return the created file
     */
    public File portFile(int port) {
      return new File(mCoordinationDir, Integer.toString(port));
    }

    /**
     * Resources used to reserve a port.
     */
    private static class Reservation {
      private final File mFile;
      private final FileLock mLock;

      private Reservation(File file, FileLock lock) {
        mFile = file;
        mLock = lock;
      }
    }
  }
}
