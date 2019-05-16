package alluxio.master.file.meta.xattr;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.List;

/**
 * This class defines an interface for implementing an extended attribute which can be stored on
 * inodes.
 *
 * Attributes are stored on inodes as a simple array of bytes ({@link ByteString}) and can be
 * the implementation for encoding/decoding
 * @param <T>
 */
public interface ExtendedAttribute<T> {

  PersistenceStateAttribute PERSISTENCE_STATE = new PersistenceStateAttribute();

  /**
   * @return the full attribute with namespace and identifier
   */
  String getName();

  /**
   * @return the namespace of the attribute lies within
   */
  String getNamespace();

  /**
   * @return the identifier within the namespace of the attribute
   */
  String getIdentifier();

  /**
   * @return the number of bytes it takes to encode a single object
   */
  int getEncodedSize();

  /**
   * Encode a single object into a byte string.
   *
   * @param object the object to encode
   * @return the byte representation of the object
   */
  ByteString encode(T object);

  /**
   * Decode an object from a single byte string.
   *
   * @param bytes the bytes to decode into a single instance of
   * @return the instance of the decoded object
   * @throws IOException if the size of the byte string isn't equal to the encoding length
   */
  T decode(ByteString bytes) throws IOException;

  /**
   * Encode a list of objects into a byte string.
   *
   * @param objects the objects to encode
   * @return a bytestring with a size equal to {@link #getEncodedSize()} * {@code objects.size()}
   */
  ByteString multiEncode(List<T> objects);

  /**
   * Decode a byte string into a list of objects, preserving ordering.
   *
   * The byte string size must be a multiple of {@link #getEncodedSize()}
   *
   * @param bytes the bytes to decode
   * @return a list of the decoded objects, preserving ordering
   * @throws IOException if decoding any of the objects fails
   */
  List<T> multiDecode(ByteString bytes) throws IOException;
}
