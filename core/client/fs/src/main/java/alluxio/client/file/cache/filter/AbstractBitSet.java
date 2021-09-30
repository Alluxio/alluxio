package alluxio.client.file.cache.filter;

public abstract class AbstractBitSet {
  abstract public boolean get(int index);

  abstract public void set(int index);

  abstract public void clear(int index);

  abstract public int size();
}
