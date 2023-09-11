package alluxio.worker.dora;


import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.PageId;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.network.protocol.databuffer.DataBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.client.file.cache.CacheManager;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PagedFileWriterTest {
    private PagedFileWriter writer;
    private  PagedDoraWorker mockWorker;
    private CacheManager mockCacheManager;
    private String fileId;
    private long pageSize = 4096; // Example page size
    private String ufsPath = "/example/file.txt"; // Example UFS path
    @Before
    public void setUp() {
        mockWorker = mock(PagedDoraWorker.class);
        mockCacheManager = mock(CacheManager.class);
        fileId = "testFileId";
        writer = new PagedFileWriter(mockWorker, ufsPath, mockCacheManager, fileId, pageSize);


    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAppendByteBuffer(){
        ByteBuffer inputBuffer = ByteBuffer.wrap("Hello, World!".getBytes());
        long length = inputBuffer.remaining();
        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(true);
        long byteWritten = writer.append(inputBuffer);
        assertEquals(length, byteWritten);
    }
    @Test
    public void testAppendByteBuffer_WithException(){
        ByteBuffer inputBuffer = ByteBuffer.wrap("Hello, World!".getBytes());

        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(false);
        long byteWritten = writer.append(inputBuffer);
        assertEquals(-1, byteWritten);
    }


    @Test
    public void testAppendByteBuf_CacheManagerThrowsException() {
        ByteBuf inputBuffer = Unpooled.wrappedBuffer("Hello, World!".getBytes());

        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(false);

        // Use assertThrows to verify that an IOException is thrown
        IOException exception = assertThrows(IOException.class, () -> writer.append(inputBuffer));
        // Verify the exception message
        assertEquals("Append failed for file " + fileId, exception.getMessage());

    }

    @Test
    public void testAppendByteBuf_HandleIsNull() {
        ByteBuf inputBuffer = Unpooled.wrappedBuffer("Hello, World!".getBytes());

        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(null);
        // Use assertThrows to verify that an IOException is thrown
        IOException exception = assertThrows(IOException.class, () -> writer.append(inputBuffer));
        // Verify the exception message
        assertEquals("Cannot write data to UFS for " + ufsPath + " @" + writer.getPosition(), exception.getMessage());
    }
    @Test
    public void testAppendByteBuf_WriteDataToUFS() throws IOException {
        ByteBuf inputBuffer = Unpooled.wrappedBuffer("Hello, World!".getBytes());
        long length = inputBuffer.readableBytes();
        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(true);
        long byteWritten = writer.append(inputBuffer);
        assertEquals(length, byteWritten);

    }

    @Test
    public void testAppendDataBuffer_bytebufExist_WithException() {
        DataBuffer dataBuffer = mock(DataBuffer.class);
        when(dataBuffer.getNettyOutput()).thenReturn(Unpooled.wrappedBuffer("Hello, World!".getBytes()));

        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(false);

        // Use assertThrows to verify that an IOException is thrown
        IOException exception = assertThrows(IOException.class, () -> writer.append(dataBuffer));
        // Verify the exception message
        assertEquals("Append failed for file " + fileId, exception.getMessage());
    }
    @Test
    public void testAppendDataBuffer_bytebufExist() throws IOException {
        DataBuffer dataBuffer = mock(DataBuffer.class);
        when(dataBuffer.getNettyOutput()).thenReturn(Unpooled.wrappedBuffer("Hello, World!".getBytes()));

        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(true);

        // Use assertThrows to verify that an IOException is thrown
        long byteWritten= writer.append(dataBuffer);
        // Verify the exception message
        assertEquals(13, byteWritten);
    }

    @Test
    public void testAppendDataBuffer_bytebufIsNull() throws IOException {
        DataBuffer dataBuffer = mock(DataBuffer.class);
        when(dataBuffer.getNettyOutput()).thenReturn(null);
        when(dataBuffer.getReadOnlyByteBuffer()).thenReturn(ByteBuffer.wrap("Hello, World!".getBytes()));
        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(false);

        long byteWritten= writer.append(dataBuffer);
        // Use assertThrows to verify that an IOException is thrown
        assertEquals(-1,byteWritten);


    }
    @Test
    public void testGetPosition() throws IOException {
        ByteBuf inputBuffer = Unpooled.wrappedBuffer("Hello, World!".getBytes());
        long length = inputBuffer.readableBytes();
        ByteBuf inputBuffer2 = Unpooled.wrappedBuffer("Hello, World!".getBytes());
        long length2 = inputBuffer.readableBytes();
        DoraOpenFileHandleContainer openFileHandleContainer = mock(DoraOpenFileHandleContainer.class);
        OpenFileHandle handle = mock(OpenFileHandle.class);

        when(mockWorker.getOpenFileHandleContainer()).thenReturn(openFileHandleContainer);
        when(openFileHandleContainer.find(ufsPath)).thenReturn(handle);

        CreateFilePOptions options;
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
        when(handle.getOptions()).thenReturn(options);

        when(mockCacheManager.append(any(PageId.class),anyInt(),any(byte[].class),any(CacheContext.class))).thenReturn(true);
        long byteWritten = writer.append(inputBuffer);
        assertEquals(length, byteWritten);

        long byteWritten2 = writer.append(inputBuffer2);
        assertEquals(length2, byteWritten2);

        assertEquals(length + length2, writer.getPosition());
    }




}