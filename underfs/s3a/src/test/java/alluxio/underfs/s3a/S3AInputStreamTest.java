package alluxio.underfs.s3a;


import alluxio.retry.CountingRetry;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;

/**
 * Unit tests for the {@link S3AInputStream}.
 */
public class S3AInputStreamTest {
    private static final String BUCKET_NAME = "testBucket";
    private static final String KEY = "testKey";
    private static final byte[] testContent = new byte[]{0x01, 0x02, 0x03 ,0x04};
    private AmazonS3 mClient;
    private S3ObjectInputStream mIn[];
    private S3AInputStream testedStream;
    private InputStream[] testInputStream;
    private S3Object[] S3Object;

    @Before
    public void before() throws Exception {
        testInputStream = new InputStream[testContent.length];
        S3Object = new S3Object[testContent.length];
        mIn = new S3ObjectInputStream[testContent.length];
        mClient = Mockito.mock(AmazonS3.class);

        for(int i = 0; i < testContent.length; i ++) {
            final long pos = i;
            final int finalI = i;
            S3Object[i] = Mockito.mock(S3Object.class);
            mIn[i] = Mockito.mock(S3ObjectInputStream.class);
            Mockito.when(mClient.getObject(argThat(argument -> {
                if(argument != null) {
                    if(argument.getRange() == null)
                        return pos == 0;
                    else
                        return argument.getRange()[0] == pos;
                }
                return false;
            }))).thenReturn(S3Object[i]);
            Mockito.when(S3Object[i].getObjectContent()).thenReturn(mIn[i]);

            byte[] mockInput = Arrays.copyOfRange(testContent, i, testContent.length);
            testInputStream[i] = new ByteArrayInputStream(mockInput) ;

            Mockito.when(mIn[i].read()).thenAnswer(invocation -> {
                return testInputStream[finalI].read();
            });
            Mockito.when(mIn[i].read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
                byte[] b = invocation.getArgument(0);
                int offset = invocation.getArgument(1);
                int length = invocation.getArgument(2);

                return testInputStream[finalI].read(b, offset, length);
            });
        }
        testedStream = new S3AInputStream(BUCKET_NAME, KEY, mClient, 0, new CountingRetry(1));
    }

    @Test
    public void readSingleByte() throws Exception {
        Assert.assertEquals((byte) 0x01, testedStream.read());
        Assert.assertEquals((byte) 0x02, testedStream.read());
        Assert.assertEquals((byte) 0x03, testedStream.read());
        Assert.assertEquals((byte) 0x04, testedStream.read());
        Assert.assertEquals( -1, testedStream.read());
    }

    @Test
    public void skipByte() throws Exception {
        Assert.assertEquals((byte) 0x01, testedStream.read());
        testedStream.skip(1);
        Assert.assertEquals((byte) 0x03, testedStream.read());
    }

    @Test
    public void readByteArray() throws Exception {
        byte[] bytes = new byte[4];
        int readCount = testedStream.read(bytes, 0, 4);
        assertEquals(4, readCount);
        assertArrayEquals(testContent, bytes);
    }
}
