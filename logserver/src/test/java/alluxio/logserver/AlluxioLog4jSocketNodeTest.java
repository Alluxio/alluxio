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

import com.google.common.collect.ImmutableList;
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
import java.util.List;

public class AlluxioLog4jSocketNodeTest {

  /**
   * Only test the class in java.lang which can be serialized. Boolean, Byte, Character, Double,
   * Float, Integer, Long, Short, String
   */
  private List<Object> createPositiveObjectList() {
    Logger logger = Logger.getLogger("a");
    return ImmutableList.of(
        new Hashtable<>(),
        new LoggingEvent(
            "fqnOfCategoryClass",
            logger,
            Level.DEBUG,
            "message",
            new Throwable()),
        new LocationInfo("c", "b", "c", "d"),
        new ThrowableInformation(new Throwable()),
        true,
        (byte) 0x11,
        0.0,
        1.0F,
        1,
        1L,
        (short) 1,
        "string"
    );
  }

  private List<Object> createNegativeObjectList() {
    Logger logger = Logger.getLogger("a");
    return ImmutableList.of(
        new LogEvent(
            "category",
            logger,
            Level.DEBUG,
            "message",
            new Throwable()),
        new LogLevel("label", 1),
        new AdapterLogRecord(),
        new Log4JLogRecord(),
        new LogTableColumn("label"),
        Level.DEBUG,
        UtilLoggingLevel.INFO,
        new Throwable(),
        new LogLevelFormatException("message"),
        new LogTableColumnFormatException("message"),
        new PropertySetterException("string")
    );
  }

  @Test
  public void testSerializables() throws IOException {
    List<Object> serializables = createPositiveObjectList();
    for (Object object : serializables) {
      // no exception expected
      serializeThenDeserializeObject(object);
    }
  }

  @Test
  public void testNonSerializables() {
    List<Object> serializables = createNegativeObjectList();
    for (Object object : serializables) {
      // exception expected
      Assert.assertThrows(IOException.class, () -> {
        serializeThenDeserializeObject(object);
      });
    }
  }

  private void serializeThenDeserializeObject(Object object) throws IOException {
    ValidatingObjectInputStream validatingObjectInputStream = null;
    byte[] buffer;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      buffer = byteArrayOutputStream.toByteArray();
    }

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer)) {
      validatingObjectInputStream = new ValidatingObjectInputStream(byteArrayInputStream);
      setAcceptList(validatingObjectInputStream);
    } catch (IOException exception) {
      System.out.println(exception.getMessage());
    }

    try {
      Object deserializedObject = validatingObjectInputStream.readObject();
    } catch (ClassNotFoundException exception) {
      System.out.println(object.getClass() + "should be checked in the white list.");
      System.out.println(exception.getMessage());
    }
  }
}
