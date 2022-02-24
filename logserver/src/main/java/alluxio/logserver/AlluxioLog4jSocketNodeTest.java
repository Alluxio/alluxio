package alluxio.logserver;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.junit.Test;
import java.io.*;
import java.util.Hashtable;

import static alluxio.logserver.AlluxioLog4jSocketNode.setAcceptList;
import static org.junit.Assert.assertEquals;

public class AlluxioLog4jSocketNodeTest {
    private ByteArrayOutputStream byteArrayOutputStream;
    private ObjectOutputStream objectOutputStream;
    byte[] buffer;
    private ByteArrayInputStream byteArrayInputStream;
    private ValidatingObjectInputStream validatingObjectInputStream;

    private void write2Stream() throws IOException {
        byteArrayOutputStream = new ByteArrayOutputStream(100);
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    }

    private void write2BufferAndCloseOutputStream() throws IOException {
        buffer = byteArrayOutputStream.toByteArray();
        objectOutputStream.close();
        byteArrayOutputStream.close();
    }

    private void readFromInputStream() throws IOException {
        byteArrayInputStream = new ByteArrayInputStream(buffer);
        validatingObjectInputStream = new ValidatingObjectInputStream(byteArrayInputStream);
        setAcceptList(validatingObjectInputStream);
    }

    @Test
    public void testHashtable() throws IOException, ClassNotFoundException {
        Hashtable<String, Integer> numbers = new Hashtable<>();
        numbers.put("one", 1);

        write2Stream();
        objectOutputStream.writeObject(numbers);
        write2BufferAndCloseOutputStream();

        readFromInputStream();
        Hashtable<String, Integer> numbers1 = (Hashtable<String, Integer>) validatingObjectInputStream.readObject();
        assertEquals(true, numbers1 instanceof Hashtable);
    }

    @Test
    public void testLoggingEvent() throws IOException, ClassNotFoundException {
        Logger logger= Logger.getLogger("a");
        LoggingEvent event = new LoggingEvent(
                "fqnOfCategoryClass", logger, Level.DEBUG, "message", new Throwable());

        write2Stream();
        objectOutputStream.writeObject(event);
        write2BufferAndCloseOutputStream();

        readFromInputStream();
        LoggingEvent event1 = (LoggingEvent) validatingObjectInputStream.readObject();
        assertEquals(true, event1 instanceof LoggingEvent);
    }

    @Test
    public void testLocationInfo() throws IOException, ClassNotFoundException {
        LocationInfo locationInfo = new LocationInfo("c","b","c","d");
        write2Stream();
        objectOutputStream.writeObject(locationInfo);
        write2BufferAndCloseOutputStream();

        readFromInputStream();
        LocationInfo locationInfo1 =(LocationInfo) validatingObjectInputStream.readObject();

        assertEquals(true, locationInfo1 instanceof LocationInfo);
    }



    @Test
    public void testThrowableInformation() throws IOException, ClassNotFoundException {
        ThrowableInformation throwableInformation = new ThrowableInformation(new Throwable());
        write2Stream();
        objectOutputStream.writeObject(throwableInformation);
        write2BufferAndCloseOutputStream();

        readFromInputStream();
        ThrowableInformation throwableInformation1 =
                (ThrowableInformation) validatingObjectInputStream.readObject();

        assertEquals(true, throwableInformation1 instanceof ThrowableInformation);
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

        write2Stream();
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

        readFromInputStream();
        Boolean bool1 = (Boolean) validatingObjectInputStream.readObject();
        Byte byteNum1 = (Byte) validatingObjectInputStream.readObject();
        Character character1 = (Character) validatingObjectInputStream.readObject();
        Double doubleNum1 = (Double) validatingObjectInputStream.readObject();
        Float floatNum1 = (Float) validatingObjectInputStream.readObject();
        Integer integer1 = (Integer) validatingObjectInputStream.readObject();
        Long longNum1 = (Long) validatingObjectInputStream.readObject();
        Short shortNum1 = (Short) validatingObjectInputStream.readObject();
        String string1 = (String) validatingObjectInputStream.readObject();

        assertEquals(true, bool1 instanceof Boolean);
        assertEquals(true, byteNum1 instanceof Byte);
        assertEquals(true, character1 instanceof Character);
        assertEquals(true, doubleNum1 instanceof Double);
        assertEquals(true, floatNum1 instanceof Float);
        assertEquals(true, integer1 instanceof Integer);
        assertEquals(true, longNum1 instanceof Long);
        assertEquals(true, shortNum1 instanceof Short);
        assertEquals(true, string1 instanceof String);
    }

    @Test
    public void testSubclassOfLoggingEvent() throws IOException, ClassNotFoundException {
        Logger logger= Logger.getLogger("a");
        LoggingEventSubclass loggingEventSubclass = new LoggingEventSubclass(
                "fqnOfCategoryClass", logger, Level.DEBUG, "message", new Throwable());
        write2Stream();
        objectOutputStream.writeObject(loggingEventSubclass);
        write2BufferAndCloseOutputStream();
        readFromInputStream();
        LoggingEventSubclass loggingEventSubclass1 =
                (LoggingEventSubclass) validatingObjectInputStream.readObject();
        assertEquals(true, loggingEventSubclass1 instanceof LoggingEventSubclass);
    }
}

