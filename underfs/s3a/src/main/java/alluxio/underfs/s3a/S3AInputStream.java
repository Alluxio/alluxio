package alluxio.underfs.s3a;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link S3ObjectInputStream} which handles skips efficiently.
 */
@NotThreadSafe
public class S3AInputStream extends InputStream {
  private final AmazonS3 mClient;
  private final String mBucketName;
  private final String mKey;

  private S3ObjectInputStream in;
  private long mPos;

  public S3AInputStream(String bucketName, String key, AmazonS3 client) {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = 0;
  }

  @Override
  public void close() {
    closeStream();
  }

  @Override
  public int read() throws IOException {
    if (in == null) {
      openStream();
    }
    int value = in.read();
    if (value != -1) { // valid data read
      mPos++;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    if (in == null) {
      openStream();
    }
    int read = in.read(b, offset, length);
    if (read != -1) {
      mPos += read;
    }
    return read;
  }

  @Override
  public long skip(long n) {
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  private void openStream() {
    if (in != null) { // stream is already open
      return;
    }
    GetObjectRequest getReq = new GetObjectRequest(mBucketName, mKey);
    getReq.setRange(mPos);
    in = mClient.getObject(getReq).getObjectContent();
  }

  private void closeStream() {
    if (in == null) {
      return;
    }
    in.abort();
    in = null;
  }
}
