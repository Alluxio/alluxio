package tachyon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import tachyon.TestUtils;

public class UtilsTest {
  @Test
  public void writeReadByteBufferTest() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);

    List<ByteBuffer> bufs = new ArrayList<ByteBuffer>();
    bufs.add(null);
    bufs.add(ByteBuffer.allocate(0));
    bufs.add(TestUtils.getIncreasingByteBuffer(99));
    bufs.add(TestUtils.getIncreasingByteBuffer(10, 99));
    bufs.add(null);

    for (int k = 0; k < bufs.size(); k ++) {
      Utils.writeByteBuffer(bufs.get(k), dos);
    }
    ByteBuffer buf = TestUtils.getIncreasingByteBuffer(10, 99);
    buf.get();
    Utils.writeByteBuffer(buf, dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

    for (int k = 0; k < bufs.size(); k ++) {
      Assert.assertEquals(bufs.get(k), Utils.readByteBuffer(dis));
    }
    Assert.assertEquals(buf, Utils.readByteBuffer(dis));

    dos.close();
    dis.close();
  }

  @Test
  public void writeReadStringTest() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);

    List<String> strings = new ArrayList<String>();
    strings.add("");
    strings.add(null);
    strings.add("abc xyz");
    strings.add("123 789");
    strings.add("!@#$%^&*()_+}{\":?><");

    for (int k = 0; k < strings.size(); k ++) {
      Utils.writeString(strings.get(k), dos);
    }

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

    for (int k = 0; k < strings.size(); k ++) {
      Assert.assertEquals(strings.get(k), Utils.readString(dis));
    }

    dos.close();
    dis.close();
  }
}
