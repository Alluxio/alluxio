package alluxio.worker.block;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.TempBlockMeta;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

// This Test is a little different from the PagedBlockStoreCommitStore due to structure different.
// MonoBlockStore.commitBlock() will call TieredBlockStore.commitBlocked() first as commitLocal, then will call BlockMasterClient.commitBlock() as commitMaster
// TieredBlockStore.commitBlock() call TieredBLockStore.commitBlockInternal inside them wake the EventListener for listener.onCommitToLocal()
// MonoBlockStore will wake the EventListener for listener.onCommitToMaster after BlockMasterClient.commitBlock() successes
// In a nutshell two onCommit events weren't called in same domain
public class MonoBlockStoreCommitBlockTest {
    public MonoBlockStore mMonoBlockStore;
    BlockMasterClientPool mockedBlockMasterClientPool;
    BlockMasterClient mockedBlockMasterClient;
    BlockMetadataManager mBlockMetadataManager;
    BlockLockManager mBlockLockManager;
    @Spy
    TieredBlockStore mTieredBlockStore;

    Long sessoinId = 1L;
    Long blockId = 2L;
    int FIRST_TIER = 1;
    // Maybe location should be asserted as well.
    BlockStoreEventListener listener0 = new AbstractBlockStoreEventListener() {
        @Override
        public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
        }

        @Override
        public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
        }
    };
    BlockStoreEventListener listener = spy(listener0);

    @Before
    public void setup() {
        mockedBlockMasterClientPool = mock(BlockMasterClientPool.class);
        mockedBlockMasterClient = mock(BlockMasterClient.class);
        when(mockedBlockMasterClientPool.acquire()).thenReturn(mockedBlockMasterClient);
        doNothing().when(mockedBlockMasterClientPool).release(any());
        mBlockLockManager = new BlockLockManager();
        mBlockMetadataManager = BlockMetadataManager.createBlockMetadataManager();
    }

    @After
    public void tearDown() {
        try {
            mMonoBlockStore.removeBlock(sessoinId, blockId);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test
    public void CommitLocalandCommitMasterBothSuccess() {
        mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager);

        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        mMonoBlockStore.createBlock(sessoinId, blockId, FIRST_TIER, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = mMonoBlockStore.createBlockWriter(1L, blockId)) {
            // LocalCacheManager line 107 🤔
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }
        mMonoBlockStore.registerBlockStoreEventListener(listener);
        mMonoBlockStore.commitBlock(sessoinId, blockId, false);

        verify(listener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    @Test
    public void CommitLocalSuccessandCommitMasterFail() {
        try {
            doAnswer((i) -> {
                throw new AlluxioStatusException(Status.UNAVAILABLE);
            }).when(mockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(), anyString(), anyLong(), anyLong());
        } catch (Exception e) {
            System.out.println(e);
        }
        mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager);

        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        mMonoBlockStore.createBlock(sessoinId, blockId, FIRST_TIER, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = mMonoBlockStore.createBlockWriter(1L, blockId)) {
            // LocalCacheManager line 107 🤔
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }
        mMonoBlockStore.registerBlockStoreEventListener(listener);
        assertThrows(RuntimeException.class, () -> {
            mMonoBlockStore.commitBlock(sessoinId, blockId, false);
        });

        verify(listener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }


    @Test
    public void CommitLocalFailandCommitMasterSuccess() {
        mTieredBlockStore = spy(new TieredBlockStore(mBlockMetadataManager, mBlockLockManager));
        doAnswer((i) -> {
            throw new RuntimeException();
        }).when(mTieredBlockStore).commitBlockInternal(anyLong(), anyLong(), anyBoolean());

        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        mMonoBlockStore.createBlock(sessoinId, blockId, FIRST_TIER, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = mMonoBlockStore.createBlockWriter(1L, blockId)) {
            // LocalCacheManager line 107 🤔
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }
        mMonoBlockStore.registerBlockStoreEventListener(listener);
        assertThrows(RuntimeException.class, () -> {
            mMonoBlockStore.commitBlock(sessoinId, blockId, false);
        });

        verify(listener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

}
