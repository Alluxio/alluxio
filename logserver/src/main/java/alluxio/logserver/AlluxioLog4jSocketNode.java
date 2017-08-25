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

import alluxio.AlluxioRemoteLogFilter;

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
  /**
   * A reference variable of the {@link AlluxioLogServerProcess} instance.
   */
  private AlluxioLogServerProcess mLogServerProcess;
  private Socket mSocket;
  private LoggerRepository mHierarchy;

  /**
   * Constructor of {@link AlluxioLog4jSocketNode}.
   *
   * @param process main log server process
   * @param socket client socket from which to read {@link org.apache.log4j.spi.LoggingEvent}
   * @throws IOException if {@link Socket#getInputStream()} encounters an I/O error when creating
   *                     the {@link java.io.InputStream}, the socket is closed, the socket is not
   *                     connected, the socket input has been shutdown with using
   *                     {@link Socket#shutdownInput()}, or an I/O error occurs while reading the
   *                     stream header in the constructor of {@link ObjectInputStream}
   */
  public AlluxioLog4jSocketNode(AlluxioLogServerProcess process, Socket socket)
      throws IOException {
    mLogServerProcess = process;
    mSocket = socket;
  }

  @Override
  public void run() {
    LoggingEvent event;
    Logger remoteLogger;
    try (ObjectInputStream objectInputStream = new ObjectInputStream(
        new BufferedInputStream(mSocket.getInputStream()))) {
      while (true) {
        event = (LoggingEvent) objectInputStream.readObject();
        if (mHierarchy == null) {
          mHierarchy = mLogServerProcess.configureHierarchy(
              mSocket.getInetAddress(),
              event.getMDC(AlluxioRemoteLogFilter.REMOTE_LOG_MDC_APPENDER_NAME_KEY).toString());
          if (mHierarchy == null) {
            // TODO(yanqin) better handling
            continue;
          }
        }
        remoteLogger = mHierarchy.getLogger(event.getLoggerName());
        if (event.getLevel().isGreaterOrEqual(remoteLogger.getEffectiveLevel())) {
          remoteLogger.callAppenders(event);
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      // Something went wrong, cannot recover.
      throw new RuntimeException(e);
    } finally {
      if (mSocket != null) {
        try {
          mSocket.close();
        } catch (Exception e) {
          // Ignore the exception caused by closing socket.
        }
      }
    }
  }
}
