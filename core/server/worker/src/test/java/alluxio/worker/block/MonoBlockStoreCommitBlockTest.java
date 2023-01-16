package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.StorageDir;

import io.grpc.Status;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

// This Test is a little different from the PagedBlockStoreCommitStore due to structure different.
// MonoBlockStore.commitBlock() will call TieredBlockStore.commitBlocked() first as commitLocal,
// then will call BlockMasterClient.commitBlock() as commitMaster
// TieredBlockStore.commitBlock() call TieredBLockStore.commitBlockInternal inside them wake the
// EventListener for listener.onCommitToLocal()
// MonoBlockStore will wake the EventListener for listener.onCommitToMaster after
// BlockMasterClient.commitBlock() successes
// In a nutshell two onCommit events weren't called in same domain
public class MonoBlockStoreCommitBlockTest {
  public MonoBlockStore mMonoBlockStore;
  BlockMasterClientPool mMockedBlockMasterClientPool;
  BlockMasterClient mMockedBlockMasterClient;
  BlockMetadataManager mBlockMetadataManager;
  BlockLockManager mBlockLockManager;
  TieredBlockStore mTieredBlockStore;
  private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  private StorageDir mTestDir1;
  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  final Long mSessionId = 1L;
  final Long mBlockId = 2L;
  // Maybe location should be asserted as well.
  BlockStoreEventListener mListener;

  @Before
  public void setup() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());

    mMockedBlockMasterClientPool = mock(BlockMasterClientPool.class);
    mMockedBlockMasterClient = mock(BlockMasterClient.class);
    when(mMockedBlockMasterClientPool.acquire()).thenReturn(mMockedBlockMasterClient);
    doNothing().when(mMockedBlockMasterClientPool).release(any());
    mBlockLockManager = new BlockLockManager();
    mBlockMetadataManager = BlockMetadataManager.createBlockMetadataManager();

    mTestDir1 = mBlockMetadataManager.getTier(FIRST_TIER_ALIAS).getDir(0);

    mListener = mock(BlockStoreEventListener.class);
    doAnswer((i) -> {
      assertEquals(2L, i.getArguments()[0]);
      return 0;
    }).when(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    doAnswer((i) -> {
      assertEquals(2L, i.getArguments()[0]);
      return 0;
    }).when(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  @Test
  public void CommitLocalandCommitMasterBothSuccess() {
    mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager);

    prepareBlockStore();

    mMonoBlockStore.commitBlock(mSessionId, mBlockId, false);

    verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    verify(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  @Test
  public void CommitLocalSuccessandCommitMasterFail() {
    try {
      doAnswer((i) -> {
        throw new AlluxioStatusException(Status.UNAVAILABLE);
      }).when(mMockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(),
              anyString(), anyLong(), anyLong());
    } catch (Exception e) {
      System.out.println(e);
    }
    mTieredBlockStore = new TieredBlockStore(mBlockMetadataManager, mBlockLockManager);

    prepareBlockStore();

    assertThrows(RuntimeException.class, () -> {
      mMonoBlockStore.commitBlock(mSessionId, mBlockId, false);
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
      mMonoBlockStore.commitBlock(mSessionId, mBlockId, false);
    });

    verify(mListener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  public void prepareBlockStore() {
    mMonoBlockStore = new MonoBlockStore(mTieredBlockStore, mMockedBlockMasterClientPool,
             mock(UfsManager.class), new AtomicReference<>(1L));

    try {
      TieredBlockStoreTestUtils.createTempBlock(mSessionId, mBlockId, 64, mTestDir1);
    } catch (Exception e) {
      System.out.println("createTempBlockExceptiona: " + e);
    }

    byte[] data = new byte[64];
    Arrays.fill(data, (byte) 1);
    ByteBuffer buf = ByteBuffer.wrap(data);
    try (BlockWriter writer = mMonoBlockStore.createBlockWriter(mSessionId, mBlockId)) {
      writer.append(buf);
    } catch (Exception e) {
      System.out.println(e);
    }
    mMonoBlockStore.registerBlockStoreEventListener(mListener);
  }
}
