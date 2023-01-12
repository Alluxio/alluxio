package alluxio.worker.block;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Spy;

import java.io.File;
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
    TieredBlockStore mTieredBlockStore;
    private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
    private StorageDir mTestDir1;
    /** Rule to create a new temporary folder during each test. */
    @Rule
    public TemporaryFolder mTestFolder = new TemporaryFolder();

    final Long sessionId = 1L;
    final Long blockId = 2L;
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
    BlockStoreEventListener mListener = spy(listener0);

    @Before
    public void setup() throws Exception {
        File tempFolder = mTestFolder.newFolder();
        TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());

        mockedBlockMasterClientPool = mock(BlockMasterClientPool.class);
        mockedBlockMasterClient = mock(BlockMasterClient.class);
        when(mockedBlockMasterClientPool.acquire()).thenReturn(mockedBlockMasterClient);
        doNothing().when(mockedBlockMasterClientPool).release(any());
        mBlockLockManager = new BlockLockManager();
        mBlockMetadataManager = BlockMetadataManager.createBlockMetadataManager();

        mTestDir1 = mBlockMetadataManager.getTier(FIRST_TIER_ALIAS).getDir(0);
    }

    @Test
    public void CommitLocalandCommitMasterBothSuccess() {
        mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager);

        prepareBlockStore();

        mMonoBlockStore.commitBlock(sessionId, blockId, false);

        verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
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

        prepareBlockStore();

        assertThrows(RuntimeException.class, () -> {
            mMonoBlockStore.commitBlock(sessionId, blockId, false);
        });

        verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }


    @Test
    public void CommitLocalFailandCommitMasterSuccess() {
        mTieredBlockStore = spy(new TieredBlockStore(mBlockMetadataManager, mBlockLockManager));
        doAnswer((i) -> {
            throw new RuntimeException();
        }).when(mTieredBlockStore).commitBlockInternal(anyLong(), anyLong(), anyBoolean());

        prepareBlockStore();

        assertThrows(RuntimeException.class, () -> {
            mMonoBlockStore.commitBlock(sessionId, blockId, false);
        });

        verify(mListener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    public void prepareBlockStore() {
        mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mockedBlockMasterClientPool, mock(UfsManager.class), new AtomicReference<>(1L));

        try {
            TieredBlockStoreTestUtils.createTempBlock(sessionId, blockId, 64, mTestDir1);
        } catch (Exception e) {
            System.out.println("createTempBlockExceptiona: " + e);
        }

        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = mMonoBlockStore.createBlockWriter(sessionId, blockId)) {
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }
        mMonoBlockStore.registerBlockStoreEventListener(mListener);
    }

}
