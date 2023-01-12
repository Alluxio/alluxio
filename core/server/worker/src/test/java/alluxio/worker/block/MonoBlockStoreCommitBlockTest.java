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
    BlockStoreEventListener listener0 = new AbstractBlockStoreEventListener() {
        @Override
        public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
            // assertEquals(dirs.get(0).getLocation(), location);
        }

        @Override
        public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
            // assertEquals(dirs.get(0).getLocation(), location);
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
        // doAnswer((i) -> { return null; }).when(mockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(), anyString(), anyLong(), anyLong());
        // try {
        //     System.out.println("Trying to remove block in Test");
        //     mMonoBlockStore.removeBlock(sessoinId, blockId);
        // } catch (Exception e) {
        //     System.out.println("Remove block Fail");
        //     System.out.println(e);
        // }
        mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager) {
            public BlockStoreLocation commitBlockInternal(Long sessionId, Long blockId, boolean pinOnCreate) {
                TempBlockMeta tempBlockMeta = mBlockMetadataManager.getTempBlockMeta(blockId).get();
                BlockStoreLocation loc = tempBlockMeta.getBlockLocation();
                return loc;
            }

        };

        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        // doAnswer((i) -> { return null; }).when(mockedBlockMasterClient).commitBlock();

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

        // doAnswer((i) -> { return null; }).when(mockedBlockMasterClient).commitBlock();

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
        //when(mTieredBlockStore.commitBlockInternal(anyLong(), anyLong(), anyBoolean())).thenThrow(new RuntimeException());
        doAnswer((i) -> {
            throw new RuntimeException();
        }).when(mTieredBlockStore).commitBlockInternal(anyLong(), anyLong(), anyBoolean());

        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        // doAnswer((i) -> { return null; }).when(mockedBlockMasterClient).commitBlock();

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
