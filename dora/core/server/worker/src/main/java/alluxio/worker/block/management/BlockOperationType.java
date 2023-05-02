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

package alluxio.worker.block.management;

/**
 * Used to specify several operation stages executed by {@link BlockManagementTask}s.
 */
public enum BlockOperationType {
    ALIGN_SWAP,             // {@link AlignTask} swap transfers.
    PROMOTE_MOVE,           // {@link PromoteTask} move transfers.
    SWAP_RESTORE_REMOVE,    // {@link SwapRestoreTask} removals.
    SWAP_RESTORE_FLUSH,     // {@link SwapRestoreTask} flush moves.
    SWAP_RESTORE_BALANCE    // {@link SwapRestoreTask} balance moves.
}
