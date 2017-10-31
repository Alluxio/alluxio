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
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.RootLogger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Reads {@link org.apache.log4j.spi.LoggingEvent} objects from remote logging
 * clients and writes the objects to designated log files. For each logging client,
 * logging server creates a {@link AlluxioLog4jSocketNode} object and starts
 * a thread serving the logging requests of the client.
 */
public class AlluxioLog4jSocketNode implements Runnable {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(AlluxioLog4jSocketNode.class);
  private static final String ROOT_LOGGER_PROPERTY_KEY = "log4j.rootLogger";
  private static final String ROOT_LOGGER_APPENDER_FILE_PROPERTY_KEY =
      "log4j.appender." + AlluxioLogServerProcess.LOGSERVER_CLIENT_LOGGER_APPENDER_NAME + ".File";
  private final String mBaseLogsDir;
  private final Socket mSocket;

  /**
   * Constructor of {@link AlluxioLog4jSocketNode}.
   *
   * Callers construct AlluxioLog4jSocketNode instances, passing the ownership of the socket
   * parameter. From now on, the AlluxioLog4jSocketNode is responsible for closing the socket.
   *
   * @param baseLogsDir base directory for logs
   * @param socket client socket from which to read {@link org.apache.log4j.spi.LoggingEvent}
   */
  public AlluxioLog4jSocketNode(String baseLogsDir, Socket socket) {
    mBaseLogsDir = Preconditions.checkNotNull(baseLogsDir,
        "Base logs directory cannot be null.");
    mSocket = Preconditions.checkNotNull(socket, "Client socket cannot be null");
  }

  @Override
  public void run() {
    try (ObjectInputStream objectInputStream = new ObjectInputStream(
        new BufferedInputStream(mSocket.getInputStream()))) {
      LoggerRepository hierarchy = null;
      while (!Thread.currentThread().isInterrupted()) {
        LoggingEvent event;
        try {
          event = (LoggingEvent) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Class of serialized object cannot be found.", e);
        } catch (IOException e) {
          throw new RuntimeException("Cannot read object from stream due to I/O error.", e);
        }
        if (hierarchy == null) {
          hierarchy = configureHierarchy(
              event.getMDC(AlluxioRemoteLogFilter.REMOTE_LOG_MDC_PROCESS_TYPE_KEY).toString());
        }
        Logger remoteLogger = hierarchy.getLogger(event.getLoggerName());
        remoteLogger.callAppenders(event);
      }
    } catch (IOException e) {
      // Something went wrong, cannot recover.
      throw new RuntimeException(e);
    } finally {
      try {
        mSocket.close();
      } catch (Exception e) {
        // Log the exception caused by closing socket.
        LOG.warn("Failed to close client socket.");
      }
    }
  }

  /**
   * Configure a {@link Hierarchy} instance used to retrive logger by name and maintain the logger
   * hierarchy. {@link AlluxioLog4jSocketNode} instance can retrieve the logger to log incoming
   * {@link org.apache.log4j.spi.LoggingEvent}s.
   *
   * @param processType type of the process sending this {@link LoggingEvent}
   * @return a {@link Hierarchy} instance to retrieve logger
   * @throws IOException if fails to create an {@link FileInputStream} to read log4j.properties
   */
  private LoggerRepository configureHierarchy(String processType)
      throws IOException {
    String inetAddressStr = mSocket.getInetAddress().getHostAddress();
    Properties properties = new Properties();
    File configFile;
    try {
      configFile = new File(new URI(System.getProperty("log4j.configuration")));
    } catch (URISyntaxException e) {
      // Alluxio log server cannot derive a valid path to log4j.properties. Since this
      // properties file is global, we should throw an exception.
      throw new RuntimeException("Cannot derive a valid URI to log4j.properties file.", e);
    }
    try (FileInputStream inputStream = new FileInputStream(configFile)) {
      properties.load(inputStream);
    }
    // Assign Level.TRACE to level so that log server prints whatever log messages received from
    // log clients. If the log server receives a LoggingEvent, it assumes that the remote
    // log client has the intention of writing this LoggingEvent to logs. It does not make
    // much sense for the log client to send the message over the network and wants the
    // messsage to be discarded by the log server.
    // With this assumption, since TRACE is the lowest logging level, we do not have to compare
    // the level of the event and the level of the logger when the log server receives
    // LoggingEvents.
    Level level = Level.TRACE;
    Hierarchy clientHierarchy = new Hierarchy(new RootLogger(level));
    // Startup script should guarantee that mBaseLogsDir already exists.
    String logDirectoryPath =
        PathUtils.concatPath(mBaseLogsDir, processType.toLowerCase());
    File logDirectory = new File(logDirectoryPath);
    if (!logDirectory.exists()) {
      logDirectory.mkdir();
    }
    String logFilePath = PathUtils.concatPath(logDirectoryPath, inetAddressStr + ".log");
    properties.setProperty(ROOT_LOGGER_PROPERTY_KEY,
        level.toString() + "," + AlluxioLogServerProcess.LOGSERVER_CLIENT_LOGGER_APPENDER_NAME);
    properties.setProperty(ROOT_LOGGER_APPENDER_FILE_PROPERTY_KEY, logFilePath);
    new PropertyConfigurator().doConfigure(properties, clientHierarchy);
    return clientHierarchy;
  }
}
