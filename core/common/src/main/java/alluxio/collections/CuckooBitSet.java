/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import alluxio.util.BitsUtils;

/**
 * This class is a wrapper of java's builtin BitSet.
 */
public class CuckooBitSet implements BitSet {

  /*
   * BitSets are packed into arrays of "words".  Currently, a word is
   * a long, which consists of 64 bits, requiring 6 address bits.
   * The choice of word size is determined purely by performance concerns.
   */
  private static final int ADDRESS_BITS_PER_WORD = 6;
  private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
  private static final int BIT_INDEX_MASK = BITS_PER_WORD - 1;

  /* Used to shift left or right for a partial word mask */
  private static final long WORD_MASK = 0xffffffffffffffffL;

  /**
   * The internal field corresponding to the serialField "bits".
   */
  private long[] mWords;

  /**
   * The number of words in the logical size of this BitSet.
   */
  private transient int mWordsInUse = 0;

  /**
   * Given a bit index, return word index containing it.
   */
  private static int wordIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }

  /**
   * Creates a new bit set. All bits are initially {@code false}.
   *
   * @param nbits the number of bits
   */
  public CuckooBitSet(int nbits) {
    // nbits can't be negative; size 0 is OK
    if (nbits < 0) {
      throw new NegativeArraySizeException("nbits < 0: " + nbits);
    }

    initWords(nbits);
  }

  private void initWords(int nbits) {
    mWords = new long[wordIndex(nbits - 1) + 1];
    mWordsInUse = mWords.length;
  }

  @Override
  public boolean get(int bitIndex) {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
    }

    int wordIndex = wordIndex(bitIndex);
    return (wordIndex < mWordsInUse)
        && ((mWords[wordIndex] & (1L << bitIndex)) != 0);
  }

  /**
   * Get the bits at the specified index with given length.
   * @param fromIndex the start index
   * @param length the length of the bits to get
   * @return the bits at the specified index with given length
   */
  public long get(int fromIndex, int length) {
    int toIndex = fromIndex + length;

    int len = length();

    // If no set bits in range return empty bitset
    if (len <= fromIndex || fromIndex == toIndex) {
      return 0L;
    }

    // An optimization
    if (toIndex > len) {
      toIndex = len;
    }

    int startWordIndex = wordIndex(fromIndex);
    int endWordIndex   = wordIndex(toIndex - 1);

    long value;
    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      int fromBitIndex = fromIndex & BIT_INDEX_MASK;
      int toBitIndex = toIndex & BIT_INDEX_MASK;
      toBitIndex = toBitIndex == 0 ? (BIT_INDEX_MASK + 1) : toBitIndex;
      value = BitsUtils.getValueFromWord(mWords[startWordIndex], fromBitIndex, toBitIndex);
    } else {
      // Case 2: Multiple words
      // Handle first word
      int fromBitIndex = fromIndex & BIT_INDEX_MASK;
      int len1 = BITS_PER_WORD - fromBitIndex;
      long lowerBits = BitsUtils.getValueFromWord(mWords[startWordIndex], fromBitIndex,
          BITS_PER_WORD);
      // Handle last word (restores invariants)
      long upperBits = BitsUtils.getValueFromWord(mWords[endWordIndex], 0,
          toIndex & BIT_INDEX_MASK);
      value = (upperBits << len1) | lowerBits;
    }

    return value;
  }

  @Override
  public void set(int bitIndex) {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
    }

    int wordIndex = wordIndex(bitIndex);
    mWords[wordIndex] |= (1L << bitIndex); // Restores invariants
  }

  /**
   * Set the bits to 1 at the specified index with given length.
   * @param fromIndex the start index
   * @param length the length of the bits to set
   */
  public void set(int fromIndex, int length) {
    int toIndex = fromIndex + length;
    if (fromIndex == toIndex) {
      return;
    }

    // Increase capacity if necessary
    int startWordIndex = wordIndex(fromIndex);
    int endWordIndex   = wordIndex(toIndex - 1);

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask  = WORD_MASK >>> -toIndex;
    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      mWords[startWordIndex] |= (firstWordMask & lastWordMask);
    } else {
      // Case 2: Multiple words
      // Handle first word
      mWords[startWordIndex] |= firstWordMask;

      // Handle intermediate words, if any
      for (int i = startWordIndex + 1; i < endWordIndex; i++) {
        mWords[i] = WORD_MASK;
      }

      // Handle last word (restores invariants)
      mWords[endWordIndex] |= lastWordMask;
    }
  }

  /**
   * Set the bits to some value at the specified index with given length.
   * @param fromIndex the start index
   * @param length the length of the bits to set
   * @param value the value to set the bits to
   */
  public void set(int fromIndex, int length, long value) {
    int toIndex = fromIndex + length;
    if (fromIndex == toIndex) {
      return;
    }

    int startWordIndex = wordIndex(fromIndex);
    int endWordIndex   = wordIndex(toIndex - 1);

    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      int fromBitIndex = fromIndex & BIT_INDEX_MASK;
      int toBitIndex = toIndex & BIT_INDEX_MASK;
      toBitIndex = toBitIndex == 0 ? BITS_PER_WORD : toBitIndex;
      mWords[startWordIndex] = BitsUtils.mergeWord(mWords[startWordIndex], value, fromBitIndex,
          toBitIndex);
    } else {
      // Case 2: Multiple words
      // Handle first word
      int fromBitIndex = fromIndex & BIT_INDEX_MASK;
      int len1 = BITS_PER_WORD - fromBitIndex;
      mWords[startWordIndex] = BitsUtils.mergeWord(mWords[startWordIndex], value,
          fromBitIndex, BITS_PER_WORD);
      // Handle second word (restores invariants)
      mWords[endWordIndex] = BitsUtils.mergeWord(mWords[endWordIndex], value >> len1, 0,
          toIndex & BIT_INDEX_MASK);
    }
  }

  /**
   * Clear the bit to 0 at the specified index.
   * @param bitIndex the index of the bit to clear
   */
  @Override
  public void clear(int bitIndex) {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
    }

    int wordIndex = wordIndex(bitIndex);
    if (wordIndex >= mWordsInUse) {
      return;
    }

    mWords[wordIndex] &= ~(1L << bitIndex);
  }

  /**
   * Clear the bits at the specified index with given length.
   * @param fromIndex the start index
   * @param length the length of the bits to clear
   */
  public void clear(int fromIndex, int length) {
    int toIndex = fromIndex + length;

    if (fromIndex == toIndex) {
      return;
    }

    int startWordIndex = wordIndex(fromIndex);
    if (startWordIndex >= mWordsInUse) {
      return;
    }

    int endWordIndex = wordIndex(toIndex - 1);
    if (endWordIndex >= mWordsInUse) {
      toIndex = length();
      endWordIndex = mWordsInUse - 1;
    }

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask  = WORD_MASK >>> -toIndex;
    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      mWords[startWordIndex] &= ~(firstWordMask & lastWordMask);
    } else {
      // Case 2: Multiple words
      // Handle first word
      mWords[startWordIndex] &= ~firstWordMask;

      // Handle intermediate words, if any
      for (int i = startWordIndex + 1; i < endWordIndex; i++) {
        mWords[i] = 0;
      }

      // Handle last word
      mWords[endWordIndex] &= ~lastWordMask;
    }
  }

  /**
   * Get the size of the bitset in bits.
   * @return the size of the bitset in bits
   */
  @Override
  public int size() {
    return mWords.length * BITS_PER_WORD;
  }

  /**
   * Get the length of words in the bitset,
   * the last word may be incomplete.
   * @return the length of words
   */
  public int length() {
    if (mWordsInUse == 0) {
      return 0;
    }

    return BITS_PER_WORD * (mWordsInUse - 1)
        + (BITS_PER_WORD - Long.numberOfLeadingZeros(mWords[mWordsInUse - 1]));
  }
}
