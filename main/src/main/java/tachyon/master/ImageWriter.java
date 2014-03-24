package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Class implemented this interface will be able to write image file.
 */
public interface ImageWriter {
  abstract void writeImage(DataOutputStream os) throws IOException;
}
