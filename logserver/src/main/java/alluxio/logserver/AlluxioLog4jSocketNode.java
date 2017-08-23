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

package alluxio.logserver;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * Reads {@link org.apache.log4j.spi.LoggingEvent} objects from remote logging
 * clients and writes the objects to designated log files. For each logging client,
 * logging server creates a {@link org.apache.log4j.spi.LoggingEvent} object and starts
 * a thread serving the logging requests of the client.
 */
public class AlluxioLog4jSocketNode implements Runnable {
  private Socket mSocket;
  private LoggerRepository mHierarchy;
  private ObjectInputStream mObjectInputStream;

  /**
   * Constructor of {@link AlluxioLog4jSocketNode}.
   *
   * @param socket client socket from which to read {@link org.apache.log4j.spi.LoggingEvent}
   * @param hierarchy named hierarchy of the logger, used to retrieve the logger
   * @throws IOException if {@link Socket#getInputStream()} encounters an I/O error when creating
   *                     the {@link java.io.InputStream}, the socket is closed, the socket is not
   *                     connected, the socket input has been shutdown with using
   *                     {@link Socket#shutdownInput()}, or an I/O error occurs while reading the
   *                     stream header in the constructor of {@link ObjectInputStream}
   */
  public AlluxioLog4jSocketNode(Socket socket, LoggerRepository hierarchy) throws IOException {
    mSocket = socket;
    mHierarchy = hierarchy;
    mObjectInputStream = new ObjectInputStream(
        new BufferedInputStream(mSocket.getInputStream()));
  }

  @Override
  public void run() {
    LoggingEvent event;
    Logger remoteLogger = null;
    try {
      if (mObjectInputStream != null) {
        while (true) {
          event = (LoggingEvent) mObjectInputStream.readObject();
          remoteLogger = mHierarchy.getLogger(event.getLoggerName());
          if (event.getLevel().isGreaterOrEqual(remoteLogger.getEffectiveLevel())) {
            remoteLogger.callAppenders(event);
          }
        }
      }
    } catch (Exception e) {
      if (remoteLogger != null) {
        remoteLogger.error("Closing connection...");
      }
    } finally {
      if (mObjectInputStream != null) {
        try {
          mObjectInputStream.close();
        } catch (Exception e) {
          if (remoteLogger != null) {
            remoteLogger.error("Closing inputstream...");
          }
        }
        if (mSocket != null) {
          try {
            mSocket.close();
          } catch (IOException e) {
            if (remoteLogger != null) {
              remoteLogger.error("Closing socket...");
            }
          }
        }
      }
    }
  }
}
