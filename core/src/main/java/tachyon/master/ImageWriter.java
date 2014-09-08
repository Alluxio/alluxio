package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;

/**
 * Class implemented this interface will be able to write image file.
 */
public abstract class ImageWriter {
  /**
   * Write image to the specified DataOutputStream. Use the specified ObjectWriter.
   * 
   * @param objWriter
   *          The used object writer
   * @param dos
   *          The target data output stream
   * @throws IOException
   */
  abstract void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException;

  /**
   * Write an ImageElement to the specified DataOutputStream. Use the specified ObjectWriter.
   * 
   * @param objWriter
   *          The used object writer
   * @param dos
   *          The target data output stream
   * @param ele
   *          The image element to be written
   */
  protected void writeElement(ObjectWriter objWriter, DataOutputStream dos, ImageElement ele) {
    try {
      objWriter.writeValue(dos, ele);
      dos.writeByte('\n');
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
