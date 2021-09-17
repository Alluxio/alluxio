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

package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.util.ObjectSizeCalculator.MemoryLayoutSpecification;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectSizeCalculatorTest {
  private int mArrayHeaderSize;
  private int mObjectHeaderSize;
  private int mReferenceHeaderSize;
  private int mSuperClassPaddingSize;
  private ObjectSizeCalculator mObjectSizeCalculator;

  private static final Logger LOG = LoggerFactory.getLogger(ObjectSizeCalculatorTest.class);

  private static final MemoryLayoutSpecification MEMORY_LAYOUT_SPECIFICATION =
      new MemoryLayoutSpecification() {
        @Override
        public int getArrayHeaderSize() {
          return 16;
        }

        @Override
        public int getObjectHeaderSize() {
          return 12;
        }

        @Override
        public int getObjectPadding() {
          return 8;
        }

        @Override
        public int getReferenceSize() {
          return 4;
        }

        @Override
        public int getSuperclassFieldPadding() {
          return 4;
        }
      };

  @Before
  public void setUp() {

    mArrayHeaderSize = MEMORY_LAYOUT_SPECIFICATION.getArrayHeaderSize();
    mObjectHeaderSize = MEMORY_LAYOUT_SPECIFICATION.getObjectHeaderSize();
    mReferenceHeaderSize = MEMORY_LAYOUT_SPECIFICATION.getReferenceSize();
    mSuperClassPaddingSize = MEMORY_LAYOUT_SPECIFICATION.getSuperclassFieldPadding();

    mObjectSizeCalculator = new ObjectSizeCalculator(MEMORY_LAYOUT_SPECIFICATION);
  }

  @Test
  public void testRounding() {
    assertEquals(0, roundTo(0, 8));
    assertEquals(8, roundTo(1, 8));
    assertEquals(8, roundTo(7, 8));
    assertEquals(8, roundTo(8, 8));
    assertEquals(16, roundTo(9, 8));
    assertEquals(16, roundTo(15, 8));
    assertEquals(16, roundTo(16, 8));
    assertEquals(24, roundTo(17, 8));
  }

  @Test
  public void testObjectSize() {
    assertSizeIs(mObjectHeaderSize, new Object());
  }

  static class ObjectWithFields {
    int mLength;
    int mOffset;
    int mHashcode;
    char[] mData = {};
  }

  @Test
  public void testObjectWithFields() {
    assertSizeIs(mObjectHeaderSize + 3 * 4 + mReferenceHeaderSize
        + mArrayHeaderSize, new ObjectWithFields());
  }

  public static class Class1 {
    private boolean mBool;
  }

  @Test
  public void testOneBooleanSize() {
    assertSizeIs(mObjectHeaderSize + 1, new Class1());
  }

  public static class Class2 extends Class1 {
    private int mInteger;
  }

  @Test
  public void testSimpleSubclassSize() {
    assertSizeIs(mObjectHeaderSize + roundTo(1, mSuperClassPaddingSize) + 4, new Class2());
  }

  @Test
  public void testZeroLengthArray() {
    assertSizeIs(mArrayHeaderSize, new byte[0]);
    assertSizeIs(mArrayHeaderSize, new int[0]);
    assertSizeIs(mArrayHeaderSize, new long[0]);
    assertSizeIs(mArrayHeaderSize, new Object[0]);
  }

  @Test
  public void testByteArrays() {
    assertSizeIs(mArrayHeaderSize + 1, new byte[1]);
    assertSizeIs(mArrayHeaderSize + 8, new byte[8]);
    assertSizeIs(mArrayHeaderSize + 9, new byte[9]);
  }

  @Test
  public void testCharArrays() {
    assertSizeIs(mArrayHeaderSize + 2 * 1, new char[1]);
    assertSizeIs(mArrayHeaderSize + 2 * 4, new char[4]);
    assertSizeIs(mArrayHeaderSize + 2 * 5, new char[5]);
  }

  @Test
  public void testIntArrays() {
    assertSizeIs(mArrayHeaderSize + 4 * 1, new int[1]);
    assertSizeIs(mArrayHeaderSize + 4 * 2, new int[2]);
    assertSizeIs(mArrayHeaderSize + 4 * 3, new int[3]);
  }

  @Test
  public void testLongArrays() {
    assertSizeIs(mArrayHeaderSize + 8 * 1, new long[1]);
    assertSizeIs(mArrayHeaderSize + 8 * 2, new long[2]);
    assertSizeIs(mArrayHeaderSize + 8 * 3, new long[3]);
  }

  @Test
  public void testObjectArrays() {
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 1, new Object[1]);
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 2, new Object[2]);
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 3, new Object[3]);
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 1, new String[1]);
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 2, new String[2]);
    assertSizeIs(mArrayHeaderSize + mReferenceHeaderSize * 3, new String[3]);
  }

  public static class Circular {
    Circular mCircular;
  }

  @Test
  public void testCircular() {
    Circular c1 = new Circular();
    long size = mObjectSizeCalculator.calculateObjectSize(c1);
    c1.mCircular = c1;
    assertEquals(size, mObjectSizeCalculator.calculateObjectSize(c1));
  }

  static class ConstantObject {
    private int mFirst;
    private int mSecond;
    private int mThird;
    private String mString;
  }

  static class CompositeObject {
    private ConstantObject mObj;
    private Long mLong;
  }

  @Test
  public void testConstant() {
    CompositeObject [] test = new CompositeObject[4];
    for (int i = 0; i < test.length; i++) {
      test[i] = new CompositeObject();
    }
    long size = mObjectSizeCalculator.calculateObjectSize(test);
    Set<Class<?>> testSet = new HashSet<>();
    testSet.add(ConstantObject.class);
    testSet.add(Long.class);
    ObjectSizeCalculator constantCalc = new ObjectSizeCalculator(
        MEMORY_LAYOUT_SPECIFICATION, testSet);
    assertEquals(size, constantCalc.calculateObjectSize(test));
  }

  @Test
  public void testConstantMap() {
    Map<Long, CompositeObject> test = new ConcurrentHashMap<>();
    for (long i = 0; i < 4; i++) {
      test.put(i, new CompositeObject());
    }
    long size = mObjectSizeCalculator.calculateObjectSize(test);
    Set<Class<?>> testSet = new HashSet<>();
    testSet.add(CompositeObject.class);
    testSet.add(Long.class);
    ObjectSizeCalculator constantCalc = new ObjectSizeCalculator(
        MEMORY_LAYOUT_SPECIFICATION, testSet);
    assertEquals(size, constantCalc.calculateObjectSize(test));
  }

  static class ComplexObject<T> {
    static class Node<T> {
      final T mVal;
      Node<T> mPrev;
      Node<T> mNext;

      Node(T value) {
        mVal = value;
      }
    }

    private Node<T> mFirst;
    private Node<T> mLast;

    void add(T item) {
      Node<T> node = new Node<T>(item);
      if (mFirst == null) {
        mFirst = node;
      } else {
        mLast.mNext = node;
        node.mPrev = mLast;
      }
      mLast = node;
    }
  }

  @Test
  public void testComplexObject() {
    ComplexObject<Object> l = new ComplexObject<Object>();
    l.add(new Object());
    l.add(new Object());
    l.add(new Object());

    long expectedSize = 0;
    // The complex object itself plus first and last refs.
    expectedSize += roundTo(mObjectHeaderSize + 2 * mReferenceHeaderSize, 8);
    // 3 Nodes - each with 3 object references.
    expectedSize += roundTo(mObjectHeaderSize + 3 * mReferenceHeaderSize, 8) * 3;
    // 3 vanilla objects contained in the node values.
    expectedSize += roundTo(mObjectHeaderSize, 8) * 3;

    assertSizeIs(expectedSize, l);
  }

  private void assertSizeIs(long size, Object o) {
    assertEquals(roundTo(size, 8), mObjectSizeCalculator.calculateObjectSize(o));
  }

  private static long roundTo(long x, int multiple) {
    return ObjectSizeCalculator.roundTo(x, multiple);
  }
}
