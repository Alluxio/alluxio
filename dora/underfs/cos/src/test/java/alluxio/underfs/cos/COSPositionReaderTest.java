package alluxio.underfs.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectInputStream;
import com.qcloud.cos.model.GetObjectRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

public class COSPositionReaderTest {
  /**
   * The COS Position Reader.
   */
  private COSPositionReader mCOSPositionReader;
  /**
   * The COS Client.
   */
  private COSClient mClient;
  /**
   * The Bucket Name.
   */
  private final String mBucketName = "bucket";
  /**
   * The Path (or the Key).
   */
  private final String mPath = "path";
  /**
   * The File Length.
   */
  private final long mFileLength = 100L;

  @Before
  public void before() throws Exception {
    mClient = Mockito.mock(COSClient.class);
    mCOSPositionReader = new COSPositionReader(mClient, mBucketName, mPath, mFileLength);
  }

  /**
   * Test case for {@link COSPositionReader#openObjectInputStream(long, int)}.
   */
  @Test
  public void openObjectInputStream() throws Exception {
    COSObject object = Mockito.mock(COSObject.class);
    COSObjectInputStream objectInputStream = Mockito.mock(COSObjectInputStream.class);
    Mockito.when(mClient.getObject(ArgumentMatchers.any(
        GetObjectRequest.class))).thenReturn(object);
    Mockito.when(object.getObjectContent()).thenReturn(objectInputStream);

    // test successful open object input stream
    long position = 0L;
    int bytesToRead = 10;
    var inputStream = mCOSPositionReader.openObjectInputStream(position, bytesToRead);
    Assert.assertTrue(inputStream instanceof COSObjectInputStream);

    // test open object input stream with exception
    Mockito.when(mClient.getObject(ArgumentMatchers.any(GetObjectRequest.class)))
        .thenThrow(CosServiceException.class);
    try {
      mCOSPositionReader.openObjectInputStream(position, bytesToRead);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}
