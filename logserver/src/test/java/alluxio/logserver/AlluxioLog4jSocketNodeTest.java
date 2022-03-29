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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.executor.ExecutorServiceFactories;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.Level;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class AlluxioLog4jSocketNodeTest {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioLog4jSocketNodeTest.class);
  private static final String LOG_SERVER_HOST =
      InstancedConfiguration.defaults().getOrDefault(PropertyKey.LOGSERVER_HOSTNAME, "localhost");
  private static final int LOG_SERVER_PORT =
      InstancedConfiguration.defaults().getInt(PropertyKey.LOGSERVER_PORT);

  private static void setupLog4J() {
    Properties properties = new Properties();
    properties.setProperty("log4j.rootLogger", "ALL,socket");
    properties.setProperty("log4j.appender.socket", "org.apache.log4j.net.SocketAppender");
    properties.setProperty("log4j.appender.socket.RemoteHost", LOG_SERVER_HOST);
    properties.setProperty("log4j.appender.socket.port", Integer.toString(LOG_SERVER_PORT));
    PropertyConfigurator.configure(properties);
  }

  @Test
  public void testAccept() throws Exception {
    ExecutorService runner =
        ExecutorServiceFactories.fixedThreadPool(getClass().getName(), 1).create();
    runner.submit(() -> {
      setupLog4J();
      LOG.trace("trace");
      LOG.debug("debug");
      LOG.info("info");
      LOG.warn("warn");
      LOG.error("error");
    });

    try (ServerSocket socket = new ServerSocket(LOG_SERVER_PORT)) {
      socket.setSoTimeout(Constants.SECOND_MS * 5);
      try (Socket client = socket.accept()) {
        client.setSoTimeout(Constants.SECOND_MS * 5);
        try (ValidatingObjectInputStream mValidatingObjectInputStream =
                 new ValidatingObjectInputStream(client.getInputStream())) {
          AlluxioLog4jSocketNode.setAcceptList(mValidatingObjectInputStream);

          LoggingEvent event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          assertEquals("trace", event.getRenderedMessage());
          assertEquals(Level.TRACE, event.getLevel());

          event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          assertEquals("debug", event.getRenderedMessage());
          assertEquals(Level.DEBUG, event.getLevel());

          event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          assertEquals("info", event.getRenderedMessage());
          assertEquals(Level.INFO, event.getLevel());

          event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          assertEquals("warn", event.getRenderedMessage());
          assertEquals(Level.WARN, event.getLevel());

          event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          assertEquals("error", event.getRenderedMessage());
          assertEquals(Level.ERROR, event.getLevel());
        }
      }
    }
  }

  private static class NotAllowedClass implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int mCounter;

    public NotAllowedClass(int count) {
      mCounter = count;
    }
  }

  @Test
  public void testReject() throws Exception {
    ExecutorService runner =
        ExecutorServiceFactories.fixedThreadPool(getClass().getName(), 1).create();
    runner.submit(() -> {
      try (Socket client = new Socket(LOG_SERVER_HOST, LOG_SERVER_PORT);
          ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream())) {
        oos.writeObject(new NotAllowedClass(1));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    try (ServerSocket socket = new ServerSocket(LOG_SERVER_PORT)) {
      socket.setSoTimeout(Constants.SECOND_MS * 5);
      try (Socket client = socket.accept()) {
        client.setSoTimeout(Constants.SECOND_MS * 5);
        try (ValidatingObjectInputStream mValidatingObjectInputStream =
                 new ValidatingObjectInputStream(client.getInputStream())) {
          AlluxioLog4jSocketNode.setAcceptList(mValidatingObjectInputStream);

          assertThrows(InvalidClassException.class, mValidatingObjectInputStream::readObject);
        }
      }
    }
  }
}
