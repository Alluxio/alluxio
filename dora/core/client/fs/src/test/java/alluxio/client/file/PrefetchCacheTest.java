package alluxio.client.file;


import alluxio.PositionReader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import java.io.IOException;


public class PrefetchCacheTest {
    @Mock
    private PositionReader mockPositionReader;

    private PrefetchCache prefetchCache;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        prefetchCache = new PrefetchCache(3, 1000);
    }

    @After
    public void after(){
        prefetchCache.close();
    }

    @Test
    public void testAddTraceAndUpdateOne() {
        prefetchCache.addTrace(0, 100);
        assertEquals(100, prefetchCache.getPrefetchSize());
    }

    @Test
    public void testAddTraceAndUpdateThree() {
        prefetchCache.addTrace(0, 100);
        prefetchCache.addTrace(100, 200);
        prefetchCache.addTrace(300, 150);
        assertEquals(450, prefetchCache.getPrefetchSize());
    }

    @Test
    public void testFillWithCacheTargetPositionInCacheRange() {
        ByteBuffer outBuffer = ByteBuffer.allocate(150);
        ByteBuf cache1 = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5});

        prefetchCache.setCache(cache1);
        int bytesRead1 = prefetchCache.fillWithCache(0, outBuffer);
        assertEquals(5, bytesRead1);
        assertEquals(1,outBuffer.get(0));
        assertEquals(2,outBuffer.get(1));
        assertEquals(3,outBuffer.get(2));
        assertEquals(4,outBuffer.get(3));
        assertEquals(5,outBuffer.get(4));

    }
    @Test
    public void testFillWithCachePositionBeyondCacheEnd() {
        ByteBuffer outBuffer = ByteBuffer.allocate(150);
        int bytesRead1 = prefetchCache.fillWithCache(0, outBuffer);
        assertEquals(0,bytesRead1);
    }

    @Test
    public void testFillWithCachePositionBehindCacheStart() {
        // Simulate a scenario where the target position is behind the cache start
        ByteBuffer outBuffer = ByteBuffer.allocate(100);
        int bytesRead = prefetchCache.fillWithCache(50, outBuffer);

        // The position is behind the cache start, so no bytes should be copied
        assertEquals(0, bytesRead);
    }

    @Test
    public void testPrefetchSuccessful() throws IOException {

        when(mockPositionReader.read(anyLong(), any(ByteBuf.class), anyInt())).thenReturn(15);
        int bytesRead = prefetchCache.prefetch(mockPositionReader, 0, 15);
        assertEquals(15, bytesRead);
    }

    @Test
    public void testPrefetchExceedsFileLength() throws IOException {
        long pos = 2000;
        int minBytesToRead = 200;
        int prefetchSize = 100;

        when(mockPositionReader.read(anyLong(), any(ByteBuf.class), anyInt())).thenReturn(100);

        int bytesPrefetched = prefetchCache.prefetch(mockPositionReader, pos, minBytesToRead);

        // Verify that the prefetch size is capped by the remaining file length
        assertEquals(0, bytesPrefetched);
    }

    @Test
    public void testPrefetchWithException() throws IOException {
        when(mockPositionReader.read(anyLong(),  any(ByteBuf.class), anyInt())).thenThrow(new IOException());
        int bytesRead = prefetchCache.prefetch(mockPositionReader, 0, 15);
        assertEquals(0, bytesRead);
    }

    @Test
    public void testClose() {
        prefetchCache.close();
        assertEquals(0, prefetchCache.getPrefetchSize());
        assertEquals(0, prefetchCache.getCache().capacity());
        assertEquals(0, prefetchCache.getCacheStartPos());
    }
}