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

package alluxio.client.block;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import alluxio.wire.BlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//qiniu2
public class AlluxioBlockCache {
    private static final Logger LOG = LoggerFactory.getLogger(AlluxioBlockStore.class);
    private static Map<Long, BlockInfo> cache = new ConcurrentHashMap<Long, BlockInfo>();

    private final static long CACHE_SIZE = 100000;

    public static BlockInfo getBlockInfoCache(long blockId) {
        return cache.get(blockId);
    }

    public static void addBlockInfoCache(long blockId, BlockInfo info) {
        while (cache.size() > CACHE_SIZE) {
            for (Long k: cache.keySet()) {
                cache.remove(k);
                break;
            }
        }

        if (info.getLocations().size() > 0) {
            cache.put(blockId, info);
            LOG.debug("==== add cache for block " + blockId + " info:" + info);
        }
    }

    public static void invalidaeBlockInfoCache(long blockId) {
        cache.remove(blockId);
    }
}
