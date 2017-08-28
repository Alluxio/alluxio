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

import com.google.common.base.Preconditions;
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
 * logging server creates a {@link AlluxioLog4jSocketNode} object and starts
 * a thread serving the logging requests of the client.
 */
public class AlluxioLog4jSocketNode implements Runnable {
  private static org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(AlluxioLog4jSocketNode.class);
  private final AlluxioLogServerProcess mLogServerProcess;
  private final Socket mSocket;

  /**
   * Constructor of {@link AlluxioLog4jSocketNode}.
   *
   * Callers construct AlluxioLog4jSocketNode instances, passing the ownership of the socket
   * parameter. From now on, the AlluxioLog4jSocketNode is responsible for closing the socket.
   *
   * @param process main log server process
   * @param socket client socket from which to read {@link org.apache.log4j.spi.LoggingEvent}
   */
  public AlluxioLog4jSocketNode(AlluxioLogServerProcess process, Socket socket) {
    mLogServerProcess = Preconditions.checkNotNull(process,
        "The log server process could not be null.");
    mSocket = Preconditions.checkNotNull(socket, "Client socket could not be null");
  }

  @Override
  public void run() {
    LoggerRepository hierarchy = null;
    LoggingEvent event;
    Logger remoteLogger;
    try (ObjectInputStream objectInputStream = new ObjectInputStream(
        new BufferedInputStream(mSocket.getInputStream()))) {
      // Check for interrupt status of current thread and preserve it.
      while (!Thread.currentThread().isInterrupted()) {
        event = (LoggingEvent) objectInputStream.readObject();
        if (hierarchy == null) {
          hierarchy = mLogServerProcess.configureHierarchy(
              mSocket.getInetAddress(),
              event.getMDC(AlluxioRemoteLogFilter.REMOTE_LOG_MDC_APPENDER_NAME_KEY).toString());
        }
        remoteLogger = hierarchy.getLogger(event.getLoggerName());
        if (event.getLevel().isGreaterOrEqual(remoteLogger.getEffectiveLevel())) {
          remoteLogger.callAppenders(event);
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      // Something went wrong, cannot recover.
      throw new RuntimeException(e);
    } finally {
      try {
        mSocket.close();
      } catch (Exception e) {
        // Ignore the exception caused by closing socket.
      }
    }
  }
}
