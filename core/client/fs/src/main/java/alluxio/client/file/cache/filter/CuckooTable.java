package alluxio.client.file.cache.filter;

public interface CuckooTable {
    public int readTag(int i, int j);

    public void writeTag(int i, int j, int t);

    public boolean findTagInBucket(int i, int tag);

    public boolean findTagInBuckets(int i1, int i2, int tag);

    public boolean deleteTagFromBucket(int i, int tag);

    public int insertOrKickoutOne(int i, int tag);

    public boolean findTagInBucket(int i, int tag, TagPosition position);

    public boolean findTagInBuckets(int i1, int i2, int tag, TagPosition position);

    public boolean deleteTagFromBucket(int i, int tag, TagPosition position);

    public int insertOrKickoutOne(int i, int tag, TagPosition position);

    public boolean insert(int i, int tag, TagPosition position);

    public int numTagsPerBuckets();

    public int numBuckets();

    public int bitsPerTag();

    public int sizeInBytes();

    public int sizeInTags();
}
