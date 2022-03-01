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

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.config.PropertySetterException;
import org.apache.log4j.helpers.UtilLoggingLevel;
import org.apache.log4j.lf5.Log4JLogRecord;
import org.apache.log4j.lf5.LogLevel;
import org.apache.log4j.lf5.LogLevelFormatException;
import org.apache.log4j.lf5.util.AdapterLogRecord;
import org.apache.log4j.lf5.viewer.LogTableColumn;
import org.apache.log4j.lf5.viewer.LogTableColumnFormatException;
import org.apache.log4j.pattern.LogEvent;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class AlluxioLog4jSocketNodeTest {

  private byte[] mBuffer;
  private ByteArrayOutputStream mByteArrayOutputStream;
  private ObjectOutputStream mObjectOutputStream;
  private ValidatingObjectInputStream mValidatingObjectInputStream;

  /**
   * Only test the class in java.lang which can be serialized. Boolean, Byte, Character, Double,
   * Float, Integer, Long, Short, String
   */
  private List<Object> createPositiveObjectList() {
    List<Object> positiveObjectList = new ArrayList<>();
    Logger logger = Logger.getLogger("a");
    positiveObjectList.add(new Hashtable<>());
    positiveObjectList.add(
        new LoggingEvent(
            "fqnOfCategoryClass",
            logger,
            Level.DEBUG,
            "message",
            new Throwable()));
    positiveObjectList.add(new LocationInfo("c", "b", "c", "d"));
    positiveObjectList.add(new ThrowableInformation(new Throwable()));
    positiveObjectList.add(true);
    positiveObjectList.add((Byte) (byte) 0x11);
    positiveObjectList.add((Double) 0.0);
    positiveObjectList.add((Float) 1.0F);
    positiveObjectList.add(1);
    positiveObjectList.add((Long) 1L);
    positiveObjectList.add((Short) (short) 1);
    positiveObjectList.add("string");
    return positiveObjectList;
  }

  private List<Object> createNegativeObjectList() {
    List<Object> positiveObjectList = new ArrayList<>();
    Logger logger = Logger.getLogger("a");
    positiveObjectList.add(new LogEvent(
        "category",
        logger,
        Level.DEBUG,
        "message",
        new Throwable()));
    positiveObjectList.add(new LogLevel("lable", 1));
    positiveObjectList.add(new AdapterLogRecord());
    positiveObjectList.add(new Log4JLogRecord());
    positiveObjectList.add(new LogTableColumn("label"));
    positiveObjectList.add(Level.DEBUG);
    positiveObjectList.add(UtilLoggingLevel.INFO);
    positiveObjectList.add(new Throwable());
    positiveObjectList.add(new LogLevelFormatException("message"));
    positiveObjectList.add(new LogTableColumnFormatException("message"));
    positiveObjectList.add(new PropertySetterException("string"));
    return positiveObjectList;
  }

  private void createOutputStreams() {
    try {
      mByteArrayOutputStream = new ByteArrayOutputStream();
      mObjectOutputStream = new ObjectOutputStream(mByteArrayOutputStream);
    } catch (IOException exception) {
      System.out.println(exception.getMessage());
    }
  }

  private void write2BufferAndCloseOutputStream() {
    mBuffer = mByteArrayOutputStream.toByteArray();
    try {
      mObjectOutputStream.close();
      mByteArrayOutputStream.close();
    } catch (IOException exception) {
      System.out.println(exception.getMessage());
    }
  }

  private void createInputStreamsAndSetAcceptList() {
    try {
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(mBuffer);
      mValidatingObjectInputStream = new ValidatingObjectInputStream(byteArrayInputStream);
      setAcceptList(mValidatingObjectInputStream);
    } catch (IOException exception) {
      System.out.println(exception.getMessage());
    }
  }

  @Test
  public void testPositiveObject() throws IOException, ClassNotFoundException {
    List<Object> list = new ArrayList<>();
    list = createPositiveObjectList();
    for (Object object : list) {
      createOutputStreams();
      try {
        mObjectOutputStream.writeObject(object);
      } catch (IOException exception) {
        System.out.println(exception.getMessage());
      }
      write2BufferAndCloseOutputStream();
      createInputStreamsAndSetAcceptList();
      try {
        Object numbers1 = mValidatingObjectInputStream.readObject();
        System.out.println(numbers1.getClass());
      } catch (IOException exception) {
        System.out.println(exception.getMessage());
        System.out.println(object.getClass() + "should be checked in the white list.");
      }
    }
  }

  @Test
  public void testNegativeObject() throws IOException {
    List<Object> list = new ArrayList<>();
    list = createNegativeObjectList();
    for (Object object : list) {
      System.out.println(object.getClass());
      createOutputStreams();
      try {
        mObjectOutputStream.writeObject(object);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndSetAcceptList();
      } catch (IOException exception) {
        System.out.println(exception.getMessage());
      }
      Assert.assertThrows(InvalidClassException.class, () -> {
        Object object1 = mValidatingObjectInputStream.readObject();
      });
    }
  }
}
