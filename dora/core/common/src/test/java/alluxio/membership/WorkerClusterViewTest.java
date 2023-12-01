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

package alluxio.membership;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkerClusterViewTest {
  @Test
  public void getWorkerById() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    WorkerIdentity worker3 = WorkerIdentityTestUtils.ofLegacyId(3);
    WorkerClusterView view = new WorkerClusterView(ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2)
    ));
    assertEquals(Optional.of(worker1), view.getWorkerById(worker1).map(WorkerInfo::getIdentity));
    assertEquals(Optional.of(worker2), view.getWorkerById(worker2).map(WorkerInfo::getIdentity));
    assertEquals(Optional.empty(), view.getWorkerById(worker3));

    WorkerClusterView emptyView = new WorkerClusterView(ImmutableList.of());
    assertFalse(emptyView.getWorkerById(worker1).isPresent());
    assertFalse(emptyView.getWorkerById(worker2).isPresent());
  }

  @Test
  public void keyedByWorkerId() {
    WorkerIdentity id = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerInfo worker1 = new WorkerInfo().setIdentity(id)
        .setAddress(new WorkerNetAddress().setHost("host1"));
    WorkerInfo worker2 = new WorkerInfo().setIdentity(id)
        .setAddress(new WorkerNetAddress().setHost("host2"));
    WorkerInfo worker3 = new WorkerInfo().setIdentity(id)
        .setAddress(new WorkerNetAddress().setHost("host3"));
    List<WorkerInfo> workers = ImmutableList.of(worker1, worker2, worker3);
    assertThrows("duplicate workers not allowed", IllegalArgumentException.class,
        () -> new WorkerClusterView(workers));
  }

  @Test
  public void iteration() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    Set<WorkerInfo> workers = ImmutableSet.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2)
    );
    WorkerClusterView view = new WorkerClusterView(workers);

    Set<WorkerInfo> workersFromView = new HashSet<>();
    for (WorkerInfo w : view) {
      workersFromView.add(w);
    }
    assertEquals(workers, workersFromView);
  }

  @Test
  public void immutableIterator() {
    WorkerClusterView view = new WorkerClusterView(ImmutableList.of(
        new WorkerInfo().setIdentity(WorkerIdentityTestUtils.randomLegacyId())));
    Iterator<WorkerInfo> iter = view.iterator();
    assertTrue(iter.hasNext());
    while (iter.hasNext()) {
      iter.next();
      assertThrows(UnsupportedOperationException.class, iter::remove);
    }
  }

  @Test
  public void size() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    List<WorkerInfo> workers = ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2)
    );
    WorkerClusterView view = new WorkerClusterView(workers);
    assertEquals(workers.size(), view.size());
    assertFalse(view.isEmpty());
  }

  @Test
  public void emptyView() {
    WorkerClusterView view = new WorkerClusterView(ImmutableList.of());
    assertEquals(0, view.size());
    assertTrue(view.isEmpty());
    assertEquals(ImmutableList.of(), view.stream().collect(Collectors.toList()));
  }

  @Test
  public void equalityWorkersOrderDoesNotMatter() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    WorkerIdentity worker3 = WorkerIdentityTestUtils.ofLegacyId(3);
    List<WorkerInfo> workers = ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2),
        new WorkerInfo().setIdentity(worker3)
    );
    Instant instant = Instant.now();
    WorkerClusterView view1 = new WorkerClusterView(workers, instant);
    WorkerClusterView view2 = new WorkerClusterView(
        new ArrayList<>(Lists.reverse(workers)), instant);
    assertEquals(view1, view2);
  }

  @Test
  public void equalityDifferentSetOfWorkersAreDifferentViews() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    WorkerIdentity worker3 = WorkerIdentityTestUtils.ofLegacyId(3);
    List<WorkerInfo> workerList1 = ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2)
    );
    List<WorkerInfo> workerList2 = ImmutableList.of(
        new WorkerInfo().setIdentity(worker2),
        new WorkerInfo().setIdentity(worker3)
    );
    Instant instant = Instant.now();
    WorkerClusterView view1 = new WorkerClusterView(workerList1, instant);
    WorkerClusterView view2 = new WorkerClusterView(workerList2, instant);
    assertNotEquals(view1, view2);
  }

  @Test
  public void equalityCreationTimeIsPartOfEqualityIdentity() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    WorkerIdentity worker3 = WorkerIdentityTestUtils.ofLegacyId(3);
    List<WorkerInfo> workers = ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2),
        new WorkerInfo().setIdentity(worker3)
    );
    Instant time1 = Instant.now();
    Instant time2 = time1.plus(Duration.ofSeconds(1));
    assertNotEquals(time1, time2);
    WorkerClusterView view1 = new WorkerClusterView(workers, time1);
    WorkerClusterView view2 = new WorkerClusterView(workers, time2);
    assertNotEquals(view1, view2);
    assertTrue(Iterables.elementsEqual(view1, view2));
  }

  @Test
  public void hashCodeImpl() {
    WorkerIdentity worker1 = WorkerIdentityTestUtils.ofLegacyId(1);
    WorkerIdentity worker2 = WorkerIdentityTestUtils.ofLegacyId(2);
    WorkerIdentity worker3 = WorkerIdentityTestUtils.ofLegacyId(3);
    List<WorkerInfo> workers = ImmutableList.of(
        new WorkerInfo().setIdentity(worker1),
        new WorkerInfo().setIdentity(worker2),
        new WorkerInfo().setIdentity(worker3)
    );
    Instant time = Instant.now();

    WorkerClusterView view1 = new WorkerClusterView(workers, time);
    WorkerClusterView view2 = new WorkerClusterView(Lists.reverse(workers), time);
    assertEquals(view1, view2);
    assertEquals(view1.hashCode(), view2.hashCode());
  }
}
