package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;

import tachyon.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Class implemented this interface will be able to write image file.
 */
public abstract class ImageWriter {
  abstract void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException;

  protected void writeElement(ObjectWriter objWriter, DataOutputStream dos, ImageElement ele) {
    try {
      objWriter.writeValue(dos, ele);
      dos.writeByte('\n');
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}
