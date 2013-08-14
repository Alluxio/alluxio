package tachyon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class UtilsTest {

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

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    DataInputStream dis = new DataInputStream(is);

    for (int k = 0; k < strings.size(); k ++) {
      Assert.assertEquals(strings.get(k), Utils.readString(dis));
    }
  }
}