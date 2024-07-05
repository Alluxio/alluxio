package alluxio.master.metastore.tikv;


import alluxio.Client;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.proto.meta.InodeMeta;
import alluxio.resource.CloseableIterator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiKVException;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * File store backed by Tikv.
 */
@ThreadSafe
public class TiKVInodeStore implements InodeStore {
    private static final Logger LOG = LoggerFactory.getLogger(TiKVInodeStore.class);
    private static final String INODES_DB_NAME = "inodes-tikv";
    private static final String INODES_COLUMN = "inodes";
    private static final String EDGES_COLUMN = "edges";
    private static final String ROCKS_STORE_NAME = "InodeStore";

    private TiConfiguration mInodeConf;
    private TiSession mInodeSession;
    private RawKVClient mInodeClient;

    /**
     * Creates and initializes a rocks block store.
     *
     * @param baseDir the base directory in which to store inode metadata
     */
    public TiKVInodeStore(String baseDir) {
        String hostConf = Configuration.getString(PropertyKey.MASTER_METASTORE_INODE_TIKV_CONNECTION);
        try {
            mInodeConf = TiConfiguration.createDefault(hostConf);
            mInodeConf.setRawKVBatchWriteTimeoutInMS(30000);
            mInodeConf.setRawKVReadTimeoutInMS(20000);
            mInodeConf.setRawKVWriteTimeoutInMS(20000);
            mInodeConf.setKvMode(String.valueOf(TiConfiguration.KVMode.RAW));
            mInodeSession = TiSession.create(mInodeConf);
            mInodeClient = mInodeSession.createRawClient();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * add param hostConf for test
     */
    public TiKVInodeStore(String baseDir, String hostConf) {
        try {
            mInodeConf = TiConfiguration.createDefault(hostConf);
            mInodeConf.setRawKVBatchWriteTimeoutInMS(30000);
            mInodeSession = TiSession.create(mInodeConf);
            mInodeClient = mInodeSession.createRawClient();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void remove(Long inodeId) {
        try {
            ByteString id = ByteString.copyFrom(TiKVUtils.toByteArray(INODES_COLUMN, inodeId));
            mInodeClient.delete(id);
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeInode(MutableInode<?> inode) {
        try {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(INODES_COLUMN, inode.getId()));
            ByteString value = ByteString.copyFrom(inode.toProto().toByteArray());
            mInodeClient.put(key,value);
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WriteBatch createWriteBatch() {
        return new TiKVInodeStore.TiKVWriteBatch();
    }

    // TODO
    @Override
    public void clear() {
        LOG.info("clear TiKVInodeStore");
    }

    @Override
    public void addChild(long parentId, String childName, Long childId) {
        try {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, parentId, childName));
            ByteString value = ByteString.copyFrom(Longs.toByteArray(childId));
            mInodeClient.put(key,value);
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeChild(long parentId, String name) {
        try {
            ByteString id = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, parentId, name));
            mInodeClient.delete(id);
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
        byte[] inode;

        try {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(INODES_COLUMN, id));
            Optional<ByteString> bytes = mInodeClient.get(key);
            if (!bytes.isPresent()) {
                return Optional.empty();
            }
            inode = bytes.get().toByteArray();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
        if (inode == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(MutableInode.fromProto(InodeMeta.Inode.parseFrom(inode)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<Long> getChildIds(Long inodeId, ReadOption option) {

        ByteString bytesPrefix;
        bytesPrefix = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, inodeId));
        ListIterator<Kvrpcpb.KvPair> iter = mInodeClient.scanPrefix(bytesPrefix).listIterator();

        TiKVIter tikvIter = new TiKVIter(iter);
        Stream<Long> idStream = StreamSupport.stream(Spliterators
                .spliteratorUnknownSize(tikvIter, Spliterator.ORDERED), false);
        return CloseableIterator.noopCloseable(idStream.iterator());
    }

    @Override
    public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
        byte[] id;
        try {
            Optional<ByteString> bytes = mInodeClient
                    .get(ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, inodeId, name)));
            if (!bytes.isPresent()) {
                return Optional.empty();
            }
            id = bytes.get().toByteArray();
        } catch (TiKVException e) {
            throw new RuntimeException(e);
        }
        if (id == null) {
            return Optional.empty();
        }
        return Optional.of(Longs.fromByteArray(id));
    }

    static class TiKVIter implements Iterator<Long> {

        final ListIterator<Kvrpcpb.KvPair> mIter;

        TiKVIter(ListIterator tikvIterator) {
            mIter = tikvIterator;
        }


        @Override
        public boolean hasNext() {
            return mIter.hasNext();
        }

        @Override
        public Long next() {
            Long l = Longs.fromByteArray(mIter.next().getValue().toByteArray());
            return l;
        }
    }

    @Override
    public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
        return getChildId(inodeId, name).flatMap(id -> {
            Optional<Inode> child = get(id);
            if (!child.isPresent()) {
                LOG.warn("Found child edge {}->{}={}, but inode {} does not exist", inodeId, name,
                        id, id);
            }
            return child;
        });
    }

    @Override
    public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
        ByteString bytesPrefix;
        bytesPrefix = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, inode.getId()));
        ListIterator<Kvrpcpb.KvPair> iter = mInodeClient.scanPrefix(bytesPrefix).listIterator();
        try {
            iter.next();
        } catch (Exception e) {
            e.printStackTrace();
            return  false;
        }
        return iter.hasNext();
    }

    @Override
    public Set<EdgeEntry> allEdges() {
        Set<EdgeEntry> edges = new HashSet<>();
        ListIterator<Kvrpcpb.KvPair> iter = mInodeClient
                .scanPrefix(ByteString.copyFromUtf8(EDGES_COLUMN)).listIterator();
        while (iter.hasNext()) {
            Kvrpcpb.KvPair kv = iter.next();
            byte[] key = kv.getKey().toByteArray();
            long parentId = TiKVUtils.readLong(key, EDGES_COLUMN.length());
            String childName = new String(key, EDGES_COLUMN.length() + Longs.BYTES,
                    key.length - Longs.BYTES - EDGES_COLUMN.length());
            long childId = Longs.fromByteArray(kv.getValue().toByteArray());
            edges.add(new EdgeEntry(parentId, childName, childId));
        }
        return edges;
    }

    @Override
    public Set<MutableInode<?>> allInodes() {
        Set<MutableInode<?>> inodes = new HashSet<>();
        ListIterator<Kvrpcpb.KvPair> iter = mInodeClient
                .scanPrefix(ByteString.copyFromUtf8(INODES_COLUMN)).listIterator();
        while (iter.hasNext()) {
            Kvrpcpb.KvPair kv = iter.next();
            long key = TiKVUtils.readLong(kv.getKey().toByteArray(), INODES_COLUMN.length());
            inodes.add(getMutable(key, ReadOption.defaults()).get());
        }
        return inodes;
    }

    /**
     * The name is intentional, in order to distinguish from the {@code Iterable} interface.
     *
     * @return an iterator over stored inodes
     */
    public CloseableIterator<InodeView> getCloseableIterator() {
        ListIterator<Kvrpcpb.KvPair> iterator = mInodeClient
                .scanPrefix(ByteString.copyFromUtf8(INODES_COLUMN)).listIterator();
        return TiKVUtils.createCloseableIterator(iterator,
                (iter) -> {
                    Kvrpcpb.KvPair kv = iter.next();
                    return getMutable(Longs.fromByteArray(kv.getKey().toByteArray()), ReadOption.defaults()).get();
                }
        );
    }

    @Override
    public CheckpointName getCheckpointName() {
        return CheckpointName.TIKV_INODE_STORE;
    }   

    // TODO
    @Override
    public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
        LOG.info("Creating tikv checkpoint");
        output = new CheckpointOutputStream(output, CheckpointType.JOURNAL_ENTRY);
        output.flush();
        LOG.info("Completed tikv checkpoint");
    }   

    // TODO
    @Override
    public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
        LOG.info("Restoring tikv from checkpoint");
        Preconditions.checkState(input.getType() == CheckpointType.JOURNAL_ENTRY,
                "Unrecognized checkpoint type when restoring %s: %s", getCheckpointName(),
                input.getType());
        LOG.info("Restored tikv checkpoint");
    }

    @Override
    public boolean supportsBatchWrite() {
        return false;
    }

    private class TiKVWriteBatch implements WriteBatch {

        ConcurrentHashMap<ByteString, ByteString> mInodeMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<ByteString, ByteString> mEdgeMap = new ConcurrentHashMap<>();

        @Override
        public void writeInode(MutableInode<?> inode) {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(INODES_COLUMN, inode.getId()));
            ByteString value = ByteString.copyFrom(inode.toProto().toByteArray());
            mInodeMap.put(key,value);
        }   

        @Override
        public void removeInode(Long key) {
            ByteString k = ByteString.copyFrom(TiKVUtils.toByteArray(INODES_COLUMN, key));
            mInodeMap.remove(k);
        }

        @Override
        public void addChild(Long parentId, String childName, Long childId) {
            ByteString key = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, parentId, childName));
            ByteString value = ByteString.copyFrom(Longs.toByteArray(childId));
            mEdgeMap.put(key,value);
        }

        @Override
        public void removeChild(Long parentId, String childName) {
            ByteString k = ByteString.copyFrom(TiKVUtils.toByteArray(EDGES_COLUMN, parentId, childName));
            mEdgeMap.remove(k);
        }

        @Override
        public void commit() {
            try {
                mInodeClient.batchPut(mInodeMap);
                mInodeClient.batchPut(mEdgeMap);
            } catch (TiKVException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            mInodeMap.clear();
            mEdgeMap.clear();
        }
    }

    @Override
    public void close() {
        LOG.info("Closing TIKVInodeStore and recycling all TIKV JNI objects");
        mInodeClient.close();
        try {
            mInodeSession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("TIKVInodeStore closed");
    }


    /**
     * @return a newline-delimited string representing the state of the inode store. This is useful
     *         for debugging purposes
     */
    public String toStringEntries() {
        StringBuilder sb = new StringBuilder();

        ListIterator<Kvrpcpb.KvPair> inodeIter = mInodeClient
                .scanPrefix(ByteString.copyFromUtf8(INODES_COLUMN)).listIterator();
        while (inodeIter.hasNext()) {
            MutableInode<?> inode;
            Kvrpcpb.KvPair inodeKV = inodeIter.next();
            try {
                inode = MutableInode.fromProto(InodeMeta.Inode.parseFrom(inodeKV.getValue().toByteArray()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sb.append("Inode ").append(inodeKV.getKey().toStringUtf8().substring(INODES_COLUMN.length())).append(": ")
                    .append(inode).append("\n");
        }

        ListIterator<Kvrpcpb.KvPair> edgeIter = mInodeClient
                .scanPrefix(ByteString.copyFromUtf8(EDGES_COLUMN)).listIterator();
        while (edgeIter.hasNext()) {
            Kvrpcpb.KvPair edgeKV = edgeIter.next();
            byte[] key = edgeKV.getKey().toByteArray();
            byte[] id = new byte[Longs.BYTES];
            byte[] name = new byte[key.length - Longs.BYTES - EDGES_COLUMN.length()];
            System.arraycopy(key, EDGES_COLUMN.length(), id, 0, Longs.BYTES);
            System.arraycopy(key, EDGES_COLUMN.length() + Longs.BYTES,
                    name, 0, key.length - Longs.BYTES - EDGES_COLUMN.length());
            sb.append(String.format("<%s,%s>->%s%n", Longs.fromByteArray(id), new String(name),
                    Long.parseLong(edgeKV.getValue().toStringUtf8())));
        }

        return sb.toString();
    }

}
