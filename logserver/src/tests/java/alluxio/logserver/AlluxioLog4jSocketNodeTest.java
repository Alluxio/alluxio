package alluxio.logserver;

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

import java.io.*;
import java.util.Hashtable;

import static alluxio.logserver.AlluxioLog4jSocketNode.setAcceptList;

public class AlluxioLog4jSocketNodeTest {
    private ByteArrayOutputStream byteArrayOutputStream;
    private ObjectOutputStream objectOutputStream;
    byte[] buffer;
    private ByteArrayInputStream byteArrayInputStream;
    private ValidatingObjectInputStream validatingObjectInputStream;

    private void createOutputStreams() throws IOException {
        byteArrayOutputStream = new ByteArrayOutputStream(100);
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    }

    private void write2BufferAndCloseOutputStream() throws IOException {
        buffer = byteArrayOutputStream.toByteArray();
        objectOutputStream.close();
        byteArrayOutputStream.close();
    }

    private void createInputStreamsAndsetAcceptList() throws IOException {
        byteArrayInputStream = new ByteArrayInputStream(buffer);
        validatingObjectInputStream = new ValidatingObjectInputStream(byteArrayInputStream);
        setAcceptList(validatingObjectInputStream);
    }

    @Test
    public void testHashtable() throws IOException, ClassNotFoundException {
        Hashtable<String, Integer> numbers = new Hashtable<>();
        numbers.put("one", 1);
        createOutputStreams();
        objectOutputStream.writeObject(numbers);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        Hashtable<String, Integer> numbers1 = (Hashtable) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLoggingEvent() throws IOException, ClassNotFoundException {
        Logger logger= Logger.getLogger("a");
        LoggingEvent event = new LoggingEvent(
                "fqnOfCategoryClass", logger, Level.DEBUG, "message", new Throwable());
        createOutputStreams();
        objectOutputStream.writeObject(event);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LoggingEvent event1 = (LoggingEvent) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLocationInfo() throws IOException, ClassNotFoundException {
        LocationInfo locationInfo = new LocationInfo("c","b","c","d");
        createOutputStreams();
        objectOutputStream.writeObject(locationInfo);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LocationInfo locationInfo1 =(LocationInfo) validatingObjectInputStream.readObject();
    }

    @Test
    public void testThrowableInformation() throws IOException, ClassNotFoundException {
        ThrowableInformation throwableInformation = new ThrowableInformation(new Throwable());
        createOutputStreams();
        objectOutputStream.writeObject(throwableInformation);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        ThrowableInformation throwableInformation1 =
                (ThrowableInformation) validatingObjectInputStream.readObject();
    }

    /**
     * Only test the class in java.lang which can be serialized.
     * Boolean, Byte, Character, Double, Float, Integer, Long, Short, String
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

        objectOutputStream.writeObject(bool);
        objectOutputStream.writeObject(byteNum);
        objectOutputStream.writeObject(character);
        objectOutputStream.writeObject(doubleNum);
        objectOutputStream.writeObject(floatNum);
        objectOutputStream.writeObject(integer);
        objectOutputStream.writeObject(longNum);
        objectOutputStream.writeObject(shortNum);
        objectOutputStream.writeObject(string);

        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();

        Boolean bool1 = (Boolean) validatingObjectInputStream.readObject();
        Byte byteNum1 = (Byte) validatingObjectInputStream.readObject();
        Character character1 = (Character) validatingObjectInputStream.readObject();
        Double doubleNum1 = (Double) validatingObjectInputStream.readObject();
        Float floatNum1 = (Float) validatingObjectInputStream.readObject();
        Integer integer1 = (Integer) validatingObjectInputStream.readObject();
        Long longNum1 = (Long) validatingObjectInputStream.readObject();
        Short shortNum1 = (Short) validatingObjectInputStream.readObject();
        String string1 = (String) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogEvent() throws IOException, ClassNotFoundException {
        Logger logger= Logger.getLogger("a");
        LogEvent logEvent = new LogEvent("category", logger, Level.DEBUG,"message",new Throwable());
        createOutputStreams();
        objectOutputStream.writeObject(logEvent);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LogEvent logEvent1 = (LogEvent) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogLevel() throws IOException, ClassNotFoundException {
        LogLevel logLevel = new LogLevel("lable",1);
        createOutputStreams();
        objectOutputStream.writeObject(logLevel);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LogLevel logLevel1 = (LogLevel) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogRecord()throws IOException, ClassNotFoundException {
        AdapterLogRecord adapterLogRecord = new AdapterLogRecord();
        createOutputStreams();
        objectOutputStream.writeObject(adapterLogRecord);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        AdapterLogRecord adapterLogRecord1 = (AdapterLogRecord) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLog4JLogRecord()throws IOException, ClassNotFoundException {
        Logger logger= Logger.getLogger("a");
        Log4JLogRecord log4JLogRecord = new Log4JLogRecord();
        createOutputStreams();
        objectOutputStream.writeObject(log4JLogRecord);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        Log4JLogRecord log4JLogRecord1 = (Log4JLogRecord) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogTableColumn()throws IOException, ClassNotFoundException {
        LogTableColumn logTableColumn = new LogTableColumn("label");
        createOutputStreams();
        objectOutputStream.writeObject(logTableColumn);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LogTableColumn logTableColumn1 = (LogTableColumn) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLevel()throws IOException, ClassNotFoundException {
        Level level = Level.DEBUG;
        createOutputStreams();
        objectOutputStream.writeObject(level);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        Level level1 = (Level) validatingObjectInputStream.readObject();
    }

    @Test
    public void testUtilLoggingLevel()throws IOException, ClassNotFoundException {
        UtilLoggingLevel utilLoggingLevel = UtilLoggingLevel.INFO;
        createOutputStreams();
        objectOutputStream.writeObject(utilLoggingLevel);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        UtilLoggingLevel utilLoggingLevel1 =
                (UtilLoggingLevel) validatingObjectInputStream.readObject();
    }

    @Test
    public void testThrowable()throws IOException, ClassNotFoundException {
        Throwable throwable = new Throwable();
        createOutputStreams();
        objectOutputStream.writeObject(throwable);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        Throwable throwable1 = (Throwable) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogLevelFormatException()throws IOException, ClassNotFoundException {
        LogLevelFormatException logLevelFormatException = new LogLevelFormatException("message");
        createOutputStreams();
        objectOutputStream.writeObject(logLevelFormatException);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LogLevelFormatException logLevelFormatException1 =
                (LogLevelFormatException) validatingObjectInputStream.readObject();
    }

    @Test
    public void testLogTableColumnFormatException()throws IOException, ClassNotFoundException {
        LogTableColumnFormatException logTableColumnFormatException =
                new LogTableColumnFormatException("message");
        createOutputStreams();
        objectOutputStream.writeObject(logTableColumnFormatException);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        LogTableColumnFormatException logTableColumnFormatException1 =
                (LogTableColumnFormatException) validatingObjectInputStream.readObject();
    }

    @Test
    public void testPropertySetterException()throws IOException, ClassNotFoundException {
        PropertySetterException propertySetterException = new PropertySetterException("string");
        createOutputStreams();
        objectOutputStream.writeObject(propertySetterException);
        write2BufferAndCloseOutputStream();
        createInputStreamsAndsetAcceptList();
        PropertySetterException propertySetterException1 =
                (PropertySetterException) validatingObjectInputStream.readObject();
    }
}