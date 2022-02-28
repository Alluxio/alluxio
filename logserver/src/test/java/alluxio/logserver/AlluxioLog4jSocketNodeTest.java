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

import java.io.InvalidClassException;

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
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;

public class AlluxioLog4jSocketNodeTest {

  private ByteArrayOutputStream mByteArrayOutputStream;
  private ObjectOutputStream mObjectOutputStream;
  byte[] mBuffer;
  private ByteArrayInputStream mByteArrayInputStream;
  private ValidatingObjectInputStream mValidatingObjectInputStream;

  private void createOutputStreams() throws IOException {
    mByteArrayOutputStream = new ByteArrayOutputStream();
    mObjectOutputStream = new ObjectOutputStream(mByteArrayOutputStream);
  }

  private void write2BufferAndCloseOutputStream() throws IOException {
    mBuffer = mByteArrayOutputStream.toByteArray();
    mObjectOutputStream.close();
    mByteArrayOutputStream.close();
  }

  private void createInputStreamsAndsetAcceptList() throws IOException {
    mByteArrayInputStream = new ByteArrayInputStream(mBuffer);
    mValidatingObjectInputStream = new ValidatingObjectInputStream(mByteArrayInputStream);
    setAcceptList(mValidatingObjectInputStream);
  }

  @Test
  public void testHashtable() throws IOException, ClassNotFoundException {
    Object numbers = new Hashtable<>();
    System.out.println(numbers.getClass());
    createOutputStreams();
    mObjectOutputStream.writeObject(numbers);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Hashtable<String, Integer> numbers1 = (Hashtable) mValidatingObjectInputStream.readObject();
  }

  @Test
  public void testLoggingEvent() throws IOException, ClassNotFoundException {
    Logger logger = Logger.getLogger("a");
    LoggingEvent event = new LoggingEvent(
        "fqnOfCategoryClass", logger, Level.DEBUG, "message", new Throwable());
    createOutputStreams();
    mObjectOutputStream.writeObject(event);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    LoggingEvent event1 = (LoggingEvent) mValidatingObjectInputStream.readObject();
  }

  @Test
  public void testLocationInfo() throws IOException, ClassNotFoundException {
    Object object = new LocationInfo("c", "b", "c", "d");
    createOutputStreams();
    mObjectOutputStream.writeObject(object);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    LocationInfo locationInfo1 = (LocationInfo) mValidatingObjectInputStream.readObject();
  }

  @Test
  public void testThrowableInformation() throws IOException, ClassNotFoundException {
    ThrowableInformation throwableInformation = new ThrowableInformation(new Throwable());
    createOutputStreams();
    mObjectOutputStream.writeObject(throwableInformation);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    ThrowableInformation throwableInformation1 =
        (ThrowableInformation) mValidatingObjectInputStream.readObject();
  }

  /**
   * Only test the class in java.lang which can be serialized. Boolean, Byte, Character, Double,
   * Float, Integer, Long, Short, String
   */
  @Test
  public void testJavaLang() throws IOException, ClassNotFoundException {
    Boolean bool = new Boolean(true);
    Byte byteNum = 0x11;
    Character character = new Character('a');
    Double doubleNum = 0.0;
    Float floatNum = (float) 1;
    Integer integer = 1;
    Long longNum = (long) 1;
    Short shortNum = 1;
    String string = "test";

    createOutputStreams();

    mObjectOutputStream.writeObject(bool);
    mObjectOutputStream.writeObject(byteNum);
    mObjectOutputStream.writeObject(character);
    mObjectOutputStream.writeObject(doubleNum);
    mObjectOutputStream.writeObject(floatNum);
    mObjectOutputStream.writeObject(integer);
    mObjectOutputStream.writeObject(longNum);
    mObjectOutputStream.writeObject(shortNum);
    mObjectOutputStream.writeObject(string);

    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();

    Boolean bool1 = (Boolean) mValidatingObjectInputStream.readObject();
    Byte byteNum1 = (Byte) mValidatingObjectInputStream.readObject();
    Character character1 = (Character) mValidatingObjectInputStream.readObject();
    Double doubleNum1 = (Double) mValidatingObjectInputStream.readObject();
    Float floatNum1 = (Float) mValidatingObjectInputStream.readObject();
    Integer integer1 = (Integer) mValidatingObjectInputStream.readObject();
    Long longNum1 = (Long) mValidatingObjectInputStream.readObject();
    Short shortNum1 = (Short) mValidatingObjectInputStream.readObject();
    String string1 = (String) mValidatingObjectInputStream.readObject();
  }

  @Test
  public void testLogEvent() throws IOException, ClassNotFoundException {
    Logger logger = Logger.getLogger("a");
    LogEvent logEvent = new LogEvent("category", logger, Level.DEBUG, "message", new Throwable());
    createOutputStreams();
    mObjectOutputStream.writeObject(logEvent);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      LogEvent logEvent1 = (LogEvent) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLogLevel() throws IOException, ClassNotFoundException {
    LogLevel logLevel = new LogLevel("lable", 1);
    createOutputStreams();
    mObjectOutputStream.writeObject(logLevel);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      LogLevel logLevel1 = (LogLevel) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLogRecord() throws IOException {
    AdapterLogRecord adapterLogRecord = new AdapterLogRecord();
    createOutputStreams();
    mObjectOutputStream.writeObject(adapterLogRecord);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      AdapterLogRecord adapterLogRecord1 = (AdapterLogRecord) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLog4JLogRecord() throws IOException, ClassNotFoundException {
    Logger logger = Logger.getLogger("a");
    Log4JLogRecord log4JLogRecord = new Log4JLogRecord();
    createOutputStreams();
    mObjectOutputStream.writeObject(log4JLogRecord);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      Log4JLogRecord log4JLogRecord1 = (Log4JLogRecord) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLogTableColumn() throws IOException, ClassNotFoundException {
    LogTableColumn logTableColumn = new LogTableColumn("label");
    createOutputStreams();
    mObjectOutputStream.writeObject(logTableColumn);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      LogTableColumn logTableColumn1 = (LogTableColumn) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLevel() throws IOException, ClassNotFoundException {
    Level level = Level.DEBUG;
    createOutputStreams();
    mObjectOutputStream.writeObject(level);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      Level level1 = (Level) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testUtilLoggingLevel() throws IOException, ClassNotFoundException {
    UtilLoggingLevel utilLoggingLevel = UtilLoggingLevel.INFO;
    createOutputStreams();
    mObjectOutputStream.writeObject(utilLoggingLevel);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      UtilLoggingLevel utilLoggingLevel1 = (UtilLoggingLevel) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testThrowable() throws IOException, ClassNotFoundException {
    Throwable throwable = new Throwable();
    createOutputStreams();
    mObjectOutputStream.writeObject(throwable);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      Throwable throwable1 = (Throwable) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLogLevelFormatException() throws IOException, ClassNotFoundException {
    LogLevelFormatException logLevelFormatException = new LogLevelFormatException("message");
    createOutputStreams();
    mObjectOutputStream.writeObject(logLevelFormatException);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      LogLevelFormatException logLevelFormatException1 = (
          LogLevelFormatException) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testLogTableColumnFormatException() throws IOException, ClassNotFoundException {
    LogTableColumnFormatException logTableColumnFormatException = new LogTableColumnFormatException(
        "message");
    createOutputStreams();
    mObjectOutputStream.writeObject(logTableColumnFormatException);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      LogTableColumnFormatException logTableColumnFormatException1 = (
          LogTableColumnFormatException) mValidatingObjectInputStream.readObject();
    });
  }

  @Test
  public void testPropertySetterException() throws IOException, ClassNotFoundException {
    PropertySetterException propertySetterException = new PropertySetterException("string");
    createOutputStreams();
    mObjectOutputStream.writeObject(propertySetterException);
    write2BufferAndCloseOutputStream();
    createInputStreamsAndsetAcceptList();
    Assert.assertThrows(InvalidClassException.class, () -> {
      PropertySetterException propertySetterException1 = (
          PropertySetterException) mValidatingObjectInputStream.readObject();
    });
  }
}
