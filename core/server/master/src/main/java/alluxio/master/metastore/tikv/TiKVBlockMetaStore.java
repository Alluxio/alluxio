package alluxio.master.metastore.tikv;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;

import com.google.common.primitives.Longs;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiKVException;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Block store backed by Tikv.
 */
@ThreadSafe
public class TiKVBlockMetaStore implements BlockMetaStore {
    private static final Logger LOG = LoggerFactory.getLogger(TiKVBlockMetaStore.class);
    private static final String BLOCKS_DB_NAME = "blocks-tikv";
    private static final String BLOCK_META_COLUMN = "blockmeta";
    private static final String BLOCK_LOCATIONS_COLUMN = "blocklocations";
    private static final String ROCKS_STORE_NAME = "BlockStore";

    private final List<RocksObject> mToClose = new ArrayList<>();

    private final LongAdder mSize = new LongAdder();

    private TiConfiguration mBlockConf;
    private TiSession mBlockSession;
    private RawKVClient mBlockClient;

    /**
     * Creates and initializes a tikv block store.
     *
     * @param baseDir the base directory in which to store block store metadata
     */
    public TiKVBlockMetaStore(String baseDir) {
        String hostConf = Configuration.getString(PropertyKey.MASTER_METASTORE_INODE_TIKV_CONNECTION);
        try {
            mBlockConf = TiConfiguration.createDefault(hostConf);
            mBlockConf.setRawKVReadTimeoutInMS(20000);
            mBlockConf.setRawKVWriteTimeoutInMS(20000);
            mBlockConf.setKvMode(String.valueOf(TiConfiguration.KVMode.RAW));
            mBlockSession = TiSession.create(mBlockConf);
            mBlockClient = mBlockSession.createRawClient();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<BlockMeta> getBlock(long id) {
        byte[] meta;
        ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(BLOCK_META_COLUMN, id));
        try {
            Optional<ByteString> bytes = mBlockClient.get(key);
            if (!bytes.isPresent()) {
                return Optional.empty();
            }
            meta = bytes.get().toByteArray();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
        if (meta == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(BlockMeta.parseFrom(meta));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putBlock(long id, BlockMeta meta) {
        ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(BLOCK_META_COLUMN, id));
        ByteString value = ByteString.copyFrom(meta.toByteArray());
        try {
            Optional<ByteString> buf = mBlockClient.get(key);
            mBlockClient.put(key, value);
            if (!buf.isPresent()) {
                mSize.increment();
            }
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeBlock(long id) {
        try {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(BLOCK_META_COLUMN, id));
            Optional<ByteString> buf = mBlockClient.get(key);
            mBlockClient.delete(key);
            if (!buf.isPresent()) {
                mSize.decrement();
            }
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO
    @Override
    public void clear() {
        mSize.reset();
        LOG.info("clear TiKVBlockStore");
    }

    @Override
    public long size() {
        return mSize.longValue();
    }

    @Override
    public void close() {
        mSize.reset();
        LOG.info("Closing TiKVBlockStore and recycling all TiKV JNI objects");
        mBlockClient.close();
        try {
            mBlockSession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("TiKVBlockStore closed");
    }


    @Override
    public CloseableIterator<Block> getCloseableIterator() {
        ListIterator<Kvrpcpb.KvPair> iterator = mBlockClient
                .scanPrefix(ByteString.copyFromUtf8(BLOCK_META_COLUMN)).listIterator();

        return TiKVUtils.createCloseableIterator(iterator,
                (iter) -> {
                    Kvrpcpb.KvPair kv = iter.next();
                    byte[] key = kv.getKey().toByteArray();
                    return new Block(TiKVUtils.readLong(key, BLOCK_META_COLUMN.length()),
                            BlockMeta.parseFrom(kv.getValue().toByteArray()));
                }
        );
    }

}
