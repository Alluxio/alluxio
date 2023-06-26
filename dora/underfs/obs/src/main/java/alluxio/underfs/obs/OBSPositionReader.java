package alluxio.underfs.obs;

import alluxio.underfs.ObjectPositionReader;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link ObjectPositionReader} that reads from OBS object store.
 */
public class OBSPositionReader extends ObjectPositionReader {
  /**
   * Client for operations with OBS.
   */
  protected final ObsClient mClient;

  /**
   * @param client     the OBS client
   * @param bucketName the bucket name
   * @param path       the file path
   * @param fileLength the file length
   */
  public OBSPositionReader(ObsClient client, String bucketName, String path, long fileLength) {
    // TODO(lu) path needs to be transformed to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }

  @Override
  protected InputStream openObjectInputStream(
      long position, int bytesToRead) throws IOException {
    ObsObject object;
    try {
      GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
      getObjectRequest.setRangeStart(position);
      getObjectRequest.setRangeEnd(position + bytesToRead - 1);
      object = mClient.getObject(getObjectRequest);
    } catch (ObsException e) {
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      throw new IOException(errorMessage, e);
    }
    return object.getObjectContent();
  }
}
