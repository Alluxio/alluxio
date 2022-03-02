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
import static org.junit.Assert.assertThrows;

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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.List;

public class AlluxioLog4jSocketNodeTest {
  private static final List<Object> ACCEPTED_OBJECTS = ImmutableList.of(
      // Accepted classes used in LoggingEvent
      new Hashtable<>(),
      new LoggingEvent(
          "fqnOfCategoryClass", Logger.getLogger("a"), Level.DEBUG, "message", new Throwable()),
      new LocationInfo("c", "b", "c", "d"),
      new ThrowableInformation(new Throwable()),
      // Primitives
      // Only the following classes from java.lang are covered:
      // Boolean, Byte, Character, Double, Float, Integer, Long, Short, String
      true,
      (byte) 0x11,
      'a',
      0.0,
      1.0F,
      1,
      1L,
      (short) 1,
      "string"
  );

  private static final List<Object> REJECTED_OBJECTS = ImmutableList.of(
      new LogEvent("category", Logger.getLogger("a"), Level.DEBUG, "message", new Throwable()),
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

  @Test
  public void testSerializables() throws Exception {
    for (Object object : ACCEPTED_OBJECTS) {
      // no exception expected
      serializeThenDeserializeObject(object);
    }
  }

  @Test
  public void testNonSerializables() {
    for (Object object : REJECTED_OBJECTS) {
      // exception expected
      assertThrows(InvalidClassException.class, () -> serializeThenDeserializeObject(object));
    }
  }

  private static Object serializeThenDeserializeObject(Object object) throws Exception {
    byte[] buffer;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      buffer = byteArrayOutputStream.toByteArray();
    }

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
        ValidatingObjectInputStream validatingObjectInputStream =
          new ValidatingObjectInputStream(byteArrayInputStream)) {
      setAcceptList(validatingObjectInputStream);
      return validatingObjectInputStream.readObject();
    }
  }
}
