package alluxio.client.file.cache.filter;

import java.util.BitSet;

public class BuiltinBitSet extends AbstractBitSet {
  private BitSet bits;

  public BuiltinBitSet(int nbits) {
    bits = new BitSet(nbits);
  }

  @Override
  public boolean get(int index) {
    return bits.get(index);
  }

  @Override
  public void set(int index) {
    bits.set(index);
  }

  @Override
  public void clear(int index) {
    bits.clear(index);
  }

  @Override
  public int size() {
    return bits.size();
  }
}
