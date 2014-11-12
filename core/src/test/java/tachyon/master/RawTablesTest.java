package tachyon.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.TachyonException;

/**
 * Tests for tachyon.master.RawTables
 */
public class RawTablesTest {
  @Test
  public void writeImageTest() throws IOException, TachyonException {
    // crate the RawTables, byte buffers, and output streams
    RawTables rt = new RawTables();
    ByteBuffer bb1 = ByteBuffer.allocate(1);
    ByteBuffer bb2 = ByteBuffer.allocate(1);
    ByteBuffer bb3 = ByteBuffer.allocate(1);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    // add elements to the RawTables
    rt.addRawTable(0, 1, bb1);
    rt.addRawTable(1, 1, bb2);
    rt.addRawTable(2, 1, bb3);

    // write the image
    rt.writeImage(writer, dos);

    List<Integer> ids = Arrays.asList(0, 1, 2);
    List<Integer> columns = Arrays.asList(1, 1, 1);
    List<ByteBuffer> data = Arrays.asList(bb1, bb2, bb3);

    // decode the written bytes
    ImageElement decoded = mapper.readValue(os.toByteArray(), ImageElement.class);

    // test the decoded ImageElement
    Assert.assertEquals(ids, decoded.get("ids", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(columns, decoded.get("columns", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(data, decoded.get("data", new TypeReference<List<ByteBuffer>>() {}));
  }
}
