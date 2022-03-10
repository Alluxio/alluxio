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

import static alluxio.logserver.AlluxioLog4jSocketNode.setAcceptList;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.log4j.spi.LoggingEvent;

import org.junit.Test;

public class LoggingEventServerTest {

  @Test
  public void test() throws IOException {
    ServerSocket socket = new ServerSocket(5002);
    while (true) {
      Socket client = socket.accept();
      ValidatingObjectInputStream mValidatingObjectInputStream = new ValidatingObjectInputStream(
          client.getInputStream());
      setAcceptList(mValidatingObjectInputStream);
      int logNums = 3; //The number of log statements
      while (logNums-- > 0) {
        try {
          LoggingEvent event = (LoggingEvent) mValidatingObjectInputStream.readObject();
          System.out.println(event.getLoggerName() + ":" + event.getMessage());
        } catch (EOFException e) {
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
      break;
    }
  }
}
