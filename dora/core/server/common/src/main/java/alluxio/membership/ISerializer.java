package alluxio.membership;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface ISerializer<T> {
  public void serialize(DataOutputStream dos, T t) throws IOException;
  public T deserialize(DataInputStream dis) throws IOException;
}
