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

package alluxio.master.table;

import alluxio.collections.Pair;
import alluxio.grpc.table.Range;
import alluxio.grpc.table.Value;

import java.util.ArrayList;
import java.util.List;

// TODO(david): make this class templated

/**
 * Domain represetation to check values.
 *
 * @param <T> type of the element
 */
public abstract class Domain<T> {
  abstract boolean isInDomain(T obj);

  /**
   * Parse from protobuf domain to Domain class.
   *
   * @param domain proto representation
   * @return a Domain object
   */
  public static Domain parseFrom(alluxio.grpc.table.Domain domain) {
    if (domain.hasAllOrNone()) {
      return new AllOrNoneDomain(domain.getAllOrNone().getAll());
    }
    if (domain.hasEquatable()) {
      return new EquatableDomain(domain.getEquatable().getWhiteList(),
          domain.getEquatable().getCandidatesList());
    }
    if (domain.hasRange()) {
      return new RangeDomain(domain.getRange().getRangesList());
    }
    return new AllOrNoneDomain(false);
  }

  private static Comparable convert(Value candidate) {
    if (candidate.hasStringType()) {
      return candidate.getStringType();
    }
    if (candidate.hasBooleanType()) {
      return candidate.getBooleanType();
    }
    if (candidate.hasDoubleType()) {
      return candidate.getDoubleType();
    }
    if (candidate.hasLongType()) {
      return candidate.getLongType();
    }
    return null;
  }

  private static class AllOrNoneDomain extends Domain {
    private boolean mAll;

    public AllOrNoneDomain(boolean all) {
      super();
      mAll = all;
    }

    @Override
    boolean isInDomain(Object obj) {
      return mAll;
    }
  }

  private static class EquatableDomain<T> extends Domain<T> {
    private boolean mWhiteList;
    private List<Object> mObjects;

    public EquatableDomain(boolean whiteList, List<Value> candidatesList) {
      super();
      mWhiteList = whiteList;
      mObjects = new ArrayList<>();
      for (Value candidate: candidatesList) {
        mObjects.add(convert(candidate));
      }
    }

    @Override
    boolean isInDomain(Object obj) {
      return mWhiteList == mObjects.contains(obj);
    }
  }

  private static class RangeDomain extends Domain {
    private List<Pair<Comparable, Comparable>> mRanges;

    public RangeDomain(List<Range> rangesList) {
      super();
      mRanges = new ArrayList<>();
      for (Range range : rangesList) {
        mRanges.add(new Pair<>(convert(range.getLow()), convert(range.getHigh())));
      }
    }

    @Override
    boolean isInDomain(Object obj) {
      for (Pair<Comparable, Comparable> pair : mRanges) {
        if ((pair.getFirst() == null || pair.getFirst().compareTo(obj) <= 0)
            && (pair.getSecond() == null || pair.getSecond().compareTo(obj) >= 0)) {
          return true;
        }
      }
      return false;
    }
  }
}
