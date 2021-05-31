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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ExecutionError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains utility methods for calculating the memory usage of objects. It
 * only works on the HotSpot JVM, and infers the actual memory layout (32 bit
 * vs. 64 bit word size, compressed object pointers vs. uncompressed) from
 * best available indicators. It can reliably detect a 32 bit vs. 64 bit JVM.
 * It can only make an educated guess at whether compressed OOPs are used,
 * though; specifically, it knows what the JVM's default choice of OOP
 * compression would be based on HotSpot version and maximum heap sizes, but if
 * the choice is explicitly overridden with the <tt>-XX:{+|-}UseCompressedOops</tt> command line
 * switch, it can not detect
 * this fact and will report incorrect sizes, as it will presume the default JVM
 * behavior.
 *
 * Adapted from https://github.com/twitter-archive/commons/blob/master/src/java/com/twitter/common/objectsize/ObjectSizeCalculator.java
 */
public class ObjectSizeCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectSizeCalculator.class);
  private int mArraySize = -1;

  /**
   * Describes constant memory overheads for various constructs in a JVM implementation.
   */
  public interface MemoryLayoutSpecification {

    /**
     * Returns the fixed overhead of an array of any type or length in this JVM.
     *
     * @return the fixed overhead of an array
     */
    int getArrayHeaderSize();

    /**
     * Returns the fixed overhead of for any {@link Object} subclass in this JVM.
     *
     * @return the fixed overhead of any object
     */
    int getObjectHeaderSize();

    /**
     * Returns the quantum field size for a field owned by an object in this JVM.
     *
     * @return the quantum field size for an object
     */
    int getObjectPadding();

    /**
     * Returns the fixed size of an object reference in this JVM.
     *
     * @return the size of all object references
     */
    int getReferenceSize();

    /**
     * Returns the quantum field size for a field owned by one of an object's ancestor superclasses
     * in this JVM.
     *
     * @return the quantum field size for a superclass field
     */
    int getSuperclassFieldPadding();
  }

  private static class CurrentLayout {
    private static final MemoryLayoutSpecification SPEC =
        getEffectiveMemoryLayoutSpecification();
  }

  /**
   * Given an object, returns the total allocated size, in bytes, of the object
   * and all other objects reachable from it.  Attempts to to detect the current JVM memory layout,
   * but may fail with {@link UnsupportedOperationException};
   *
   * @param obj the object; can be null. Passing in a {@link java.lang.Class} object doesn't do
   *            anything special, it measures the size of all objects
   *            reachable through it (which will include its class loader, and by
   *            extension, all other Class objects loaded by
   *            the same loader, and all the parent class loaders). It doesn't provide the
   *            size of the static fields in the JVM class that the Class object
   *            represents.
   * @return the total allocated size of the object and all other objects it
   * retains
   * @throws UnsupportedOperationException if the current vm memory layout cannot be detected
   */
  public static long getObjectSize(Object obj)
      throws UnsupportedOperationException {
    return obj == null ? 0 : new ObjectSizeCalculator(CurrentLayout.SPEC).calculateObjectSize(obj);
  }

  /**
   * Given an object, returns the total allocated size, in bytes, of the object
   * and all other objects reachable from it.  Attempts to to detect the current JVM memory layout,
   * but may fail with {@link UnsupportedOperationException};
   *
   * @param obj the object; can be null
   * @param constantClasses a set of classes that are considered constant sized
   *
   * @return the total allocated size of the object and all other objects it
   */
  public static long getObjectSize(Object obj, Set<Class<?>> constantClasses)
      throws UnsupportedOperationException {
    return obj == null ? 0 : new ObjectSizeCalculator(CurrentLayout.SPEC, constantClasses)
        .calculateObjectSize(obj);
  }

  // Fixed object header size for arrays.
  private final int mArrayHeaderSize;
  // Fixed object header size for non-array objects.
  private final int mObjectHeaderSize;
  // Padding for the object size - if the object size is not an exact multiple
  // of this, it is padded to the next multiple.
  private final int mObjectPadding;
  // Size of reference (pointer) fields.
  private final int mReferenceSize;
  // Padding for the fields of superclass before fields of subclasses are
  // added.
  private final int mSuperClassPadding;

  private final LoadingCache<Class<?>, ClassSizeInfo> mClassInfos;

  private final Set<Object> mVisited = Sets.newIdentityHashSet();
  private final Deque<Object> mPending = new ArrayDeque<>(16 * 1024);
  private final Set<Class<?>> mConstantSizeClass;
  private final Map<Class<?>, Long> mClassSizeCache;
  private boolean mEstimateArray = false;
  private Set<Class<?>> mConstantClass = new HashSet<>();

  private long mSize;
  private int mRecursionDepth = 0;
  private static final int MAX_RECURSION = 5;

  /**
   * Creates an object size calculator that can calculate object sizes for a given
   * {@code memoryLayoutSpecification}.
   *
   * @param memoryLayoutSpecification a description of the JVM memory layout
   */
  public ObjectSizeCalculator(MemoryLayoutSpecification memoryLayoutSpecification) {
    this(memoryLayoutSpecification, Collections.EMPTY_SET);
  }

  /**
   * Creates an object size calculator that can calculate object sizes for a given
   * {@code memoryLayoutSpecification}.
   *
   * @param memoryLayoutSpecification a description of the JVM memory layout
   * @param constSizeClass constant sized classes
   */
  public ObjectSizeCalculator(MemoryLayoutSpecification memoryLayoutSpecification,
      Set<Class<?>> constSizeClass) {
    Preconditions.checkNotNull(memoryLayoutSpecification);
    Preconditions.checkNotNull(constSizeClass);
    mArrayHeaderSize = memoryLayoutSpecification.getArrayHeaderSize();
    mObjectHeaderSize = memoryLayoutSpecification.getObjectHeaderSize();
    mObjectPadding = memoryLayoutSpecification.getObjectPadding();
    mReferenceSize = memoryLayoutSpecification.getReferenceSize();
    mSuperClassPadding = memoryLayoutSpecification.getSuperclassFieldPadding();
    mConstantSizeClass = constSizeClass;
    if (!mConstantSizeClass.isEmpty()) {
      mEstimateArray = true;
    }
    mClassSizeCache = new HashMap<>();
    mClassInfos = CacheBuilder.newBuilder().build(new CacheLoader<Class<?>, ClassSizeInfo>() {
      public ClassSizeInfo load(Class<?> clazz) {
        return new ClassSizeInfo(clazz);
      }
    });
  }

  /**
   * Creates an object size calculator based on a previous one.
   *
   * @param calc previous calculator
   */
  public ObjectSizeCalculator(ObjectSizeCalculator calc) {
    mArrayHeaderSize = calc.mArrayHeaderSize;
    mObjectHeaderSize = calc.mObjectHeaderSize;
    mObjectPadding = calc.mObjectPadding;
    mReferenceSize = calc.mReferenceSize;
    mSuperClassPadding = calc.mSuperClassPadding;
    mConstantSizeClass = calc.mConstantSizeClass;
    // share class info between calculators
    mClassInfos = calc.mClassInfos;
    mClassSizeCache = calc.mClassSizeCache;
    mEstimateArray = calc.mEstimateArray;
    mRecursionDepth = calc.mRecursionDepth;
  }

  /**
   * Given an object, returns the total allocated size, in bytes, of the object
   * and all other objects reachable from it.
   *
   * @param obj the object; can be null. Passing in a {@link java.lang.Class} object doesn't do
   *            anything special, it measures the size of all objects
   *            reachable through it (which will include its class loader, and by
   *            extension, all other Class objects loaded by
   *            the same loader, and all the parent class loaders). It doesn't provide the
   *            size of the static fields in the JVM class that the Class object
   *            represents.
   * @return the total allocated size of the object and all other objects it
   * retains
   */
  public synchronized long calculateObjectSize(Object obj) {
    // Breadth-first traversal instead of naive depth-first with recursive
    // implementation, so we don't blow the stack traversing long linked lists.
    boolean init = true;
    try {
      for (;;) {
        visit(obj, init);
        init = false;
        if (mPending.isEmpty()) {
          return mSize;
        }
        obj = mPending.removeFirst();
      }
    } finally {
      mVisited.clear();
      mPending.clear();
      mSize = 0;
    }
  }

  private void visit(Object obj, boolean init) {
    if (obj == null || mVisited.contains(obj)
        || (!init && mConstantClass.contains(obj.getClass()))) {
      return;
    }
    final Class<?> clazz = obj.getClass();

    if (clazz == ArrayElementsVisitor.class) {
      ((ArrayElementsVisitor) obj).visit(this);
    } else {
      mVisited.add(obj);
      if (clazz.isArray()) {
        visitArray(obj);
      } else {
        if (Map.class.isAssignableFrom(clazz)) {
          mArraySize = ((Map) obj).size();
        }
        try {
          mClassInfos.getUnchecked(clazz).visit(obj, this);
        } catch (ExecutionError e) {
          if (!(e.getCause() instanceof NoClassDefFoundError)) {
            LOG.info("Exception occurred while calculating heap size: " + e.getMessage());
          } else {
            LOG.debug("ClassNotFound Exception occurred while calculating heap size: "
                + e.getMessage());
          }
        }
      }
    }
  }

  private void visitArray(Object array) {
    final Class<?> componentType = array.getClass().getComponentType();
    final int length = Array.getLength(array);
    if (componentType.isPrimitive()) {
      increaseByArraySize(length, getPrimitiveFieldSize(componentType));
    } else {
      increaseByArraySize(length, mReferenceSize);
      // If we didn't use an ArrayElementsVisitor, we would be enqueueing every
      // element of the array here instead. For large arrays, it would
      // tremendously enlarge the queue. In essence, we're compressing it into
      // a small command object instead. This is different than immediately
      // visiting the elements, as their visiting is scheduled for the end of
      // the current queue.
      switch (length) {
        case 0: {
          break;
        }
        case 1: {
          enqueue(Array.get(array, 0));
          break;
        }
        default: {
          enqueue(new ArrayElementsVisitor((Object[]) array, ((Object[]) array).length));
        }
      }
    }
  }

  private void increaseByArraySize(int length, long elementSize) {
    increaseSize(roundTo(mArrayHeaderSize + length * elementSize, mObjectPadding));
  }

  private long getClassSizeRecursive(Class<?> type, Object obj) {
    if (mClassSizeCache.containsKey(type)) {
      return mClassSizeCache.get(type);
    }
    if (mEstimateArray && (mRecursionDepth < MAX_RECURSION)) {
      ObjectSizeCalculator recCalc = new ObjectSizeCalculator(this);
      recCalc.mRecursionDepth++;
      recCalc.mConstantClass.add(type);
      if (type.getEnclosingClass() != null) {
        recCalc.mConstantClass.add(type.getEnclosingClass());
      }
      long size = recCalc.calculateObjectSize(obj);
      mClassSizeCache.put(type, size);
      return size;
    } else {
      throw new UnsupportedOperationException(type.toString() + " is not a constant size class");
    }
  }

  private static class ArrayElementsVisitor {
    private final Object[] mArray;

    ArrayElementsVisitor(Object[] array, long size) {
      mArray = array;
    }

    private boolean isElementConstant(ObjectSizeCalculator calc,
        Class<?> componentType, Object firstElement, int count) {
      if (count < 0) {
        return false;
      }
      if (calc.mConstantSizeClass.contains(firstElement.getClass())) {
        return true;
      }
      Field [] fields = componentType.getDeclaredFields();
      for (Field f : fields) {
        Class<?> clazz = f.getType();
        if (clazz.isPrimitive() || clazz.equals(componentType)
            || f.getType().equals(componentType.getEnclosingClass())) {
          continue;
        }
        f.setAccessible(true);
        try {
          Object obj = f.get(firstElement);
          if (obj == null || obj.getClass().isPrimitive() || obj.getClass().equals(componentType)
              || calc.mConstantSizeClass.contains(obj.getClass()) || obj == firstElement
              || isElementConstant(calc, obj.getClass(), obj, count - 1)) {
            continue;
          }
        } catch (IllegalAccessException e) {
          return false;
        }
        return false;
      }
      return true;
    }

    public void visit(ObjectSizeCalculator calc) {
      final Class<?> componentType = mArray.getClass().getComponentType();
      int arraySize = calc.mArraySize >= 0 ? calc.mArraySize : mArray.length;
      int i = 0;
      while (i < mArray.length && mArray[i] == null) {
        i++;
      }

      try {
        if (mArray != null && mArray.length != 0 && calc.mEstimateArray && i < mArray.length) {
          if (arraySize > 10000 && mArray[i] != null) {
            // When the component is a constant size data type
            if (isElementConstant(calc, componentType, mArray[i], MAX_RECURSION)) {
              long size = calc.getClassSizeRecursive(componentType, mArray[i]);
              calc.increaseSize(arraySize * size);
              calc.mArraySize = -1;
              return;
            }
          }
        }
      } catch (UnsupportedOperationException e) {
        LOG.info(e.getMessage());
      }
      for (Object elem : mArray) {
        if (elem != null) {
          calc.visit(elem, false);
        }
      }
    }
  }

  void enqueue(Object obj) {
    if (obj != null) {
      mPending.addLast(obj);
    }
  }

  void increaseSize(long objectSize) {
    mSize += objectSize;
  }

  @VisibleForTesting
  static long roundTo(long x, int multiple) {
    return ((x + multiple - 1) / multiple) * multiple;
  }

  private class ClassSizeInfo {
    // Padded fields + header size
    private final long mObjectSize;
    // Only the fields size - used to calculate the subclasses' memory
    // footprint.
    private final long mFieldSize;
    private final Field[] mReferencedFields;

    public ClassSizeInfo(Class<?> clazz) {
      long fieldsSize = 0;
      List<Field> referenceFields = new LinkedList<Field>();
      for (Field f : clazz.getDeclaredFields()) {
        if (Modifier.isStatic(f.getModifiers())) {
          continue;
        }
        final Class<?> type = f.getType();
        if (type.isPrimitive()) {
          fieldsSize += getPrimitiveFieldSize(type);
        } else {
          f.setAccessible(true);
          if (f.getDeclaringClass().equals(clazz)
              && !f.getType().equals(clazz.getEnclosingClass())) {
            referenceFields.add(f);
          }
          fieldsSize += mReferenceSize;
        }
      }
      final Class<?> superClass = clazz.getSuperclass();
      if (superClass != null) {
        final ClassSizeInfo superClassInfo = mClassInfos.getUnchecked(superClass);
        fieldsSize += roundTo(superClassInfo.mFieldSize, mSuperClassPadding);
        referenceFields.addAll(Arrays.asList(superClassInfo.mReferencedFields));
      }
      mFieldSize = fieldsSize;
      mObjectSize = roundTo(mObjectHeaderSize + fieldsSize, mObjectPadding);
      mReferencedFields = referenceFields.toArray(new Field[referenceFields.size()]);
    }

    void visit(Object obj, ObjectSizeCalculator calc) {
      calc.increaseSize(mObjectSize);
      enqueueReferencedObjects(obj, calc);
    }

    public void enqueueReferencedObjects(Object obj, ObjectSizeCalculator calc) {
      for (Field f : mReferencedFields) {
        try {
          if (!calc.mConstantClass.contains(f.getType())) {
            calc.enqueue(f.get(obj));
          }
        } catch (IllegalAccessException e) {
          final AssertionError ae = new AssertionError(
              "Unexpected denial of access to " + f);
          ae.initCause(e);
          throw ae;
        }
      }
    }
  }

  private static long getPrimitiveFieldSize(Class<?> type) {
    if (type == boolean.class || type == byte.class) {
      return 1;
    }
    if (type == char.class || type == short.class) {
      return 2;
    }
    if (type == int.class || type == float.class) {
      return 4;
    }
    if (type == long.class || type == double.class) {
      return 8;
    }
    throw new AssertionError("Encountered unexpected primitive type " + type.getName());
  }

  @VisibleForTesting
  static MemoryLayoutSpecification getEffectiveMemoryLayoutSpecification() {
    final String vmName = System.getProperty("java.vm.name");
    if (vmName == null || !(vmName.startsWith("Java HotSpot(TM) ")
        || vmName.startsWith("OpenJDK"))) {
      throw new UnsupportedOperationException(
          "ObjectSizeCalculator only supported on HotSpot VM");
    }

    final String dataModel = System.getProperty("sun.arch.data.model");
    if ("32".equals(dataModel)) {
      // Running with 32-bit data model
      return new MemoryLayoutSpecification() {
        @Override
        public int getArrayHeaderSize() {
          return 12;
        }

        @Override
        public int getObjectHeaderSize() {
          return 8;
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
    } else if (!"64".equals(dataModel)) {
      throw new UnsupportedOperationException("Unrecognized value '"
          + dataModel + "' of sun.arch.data.model system property");
    }

    final String strVmVersion = System.getProperty("java.vm.version");
    final int vmVersion = Integer.parseInt(strVmVersion.substring(0,
        strVmVersion.indexOf('.')));
    if (vmVersion >= 17) {
      long maxMemory = 0;
      for (MemoryPoolMXBean mp : ManagementFactory.getMemoryPoolMXBeans()) {
        maxMemory += mp.getUsage().getMax();
      }
      if (maxMemory < 30L * 1024 * 1024 * 1024) {
        // HotSpot 17.0 and above use compressed OOPs below 30GB of RAM total
        // for all memory pools (yes, including code cache).
        return new MemoryLayoutSpecification() {
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
      }
    }

    // In other cases, it's a 64-bit uncompressed OOPs object model
    return new MemoryLayoutSpecification() {
      @Override
      public int getArrayHeaderSize() {
        return 24;
      }

      @Override
      public int getObjectHeaderSize() {
        return 16;
      }

      @Override
      public int getObjectPadding() {
        return 8;
      }

      @Override
      public int getReferenceSize() {
        return 8;
      }

      @Override
      public int getSuperclassFieldPadding() {
        return 8;
      }
    };
  }
}
