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

package alluxio.client.file.cache;

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.FileBasedDateCacheFilter;
import alluxio.client.hive.HiveCacheContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.wire.FileInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFileBasedDateCacheFilter {
    private static FileBasedDateCacheFilter filter;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @BeforeClass
    public static void setup()
            throws IOException
    {
        filter = new FileBasedDateCacheFilter(InstancedConfiguration.defaults(), getResourceFilePath("alluxio_cache_filter.json"));
    }

    @Test()
    public void testNeedsCache()
    {
        Calendar calendar = Calendar.getInstance();
        assertTrue(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table1", null), "id"))));
        calendar.add(Calendar.DATE, -2);
        assertTrue(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table1", sdf.format(calendar.getTime())), "id"))));
        assertTrue(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table3", "2021-03-02"), "id"))));
        assertTrue(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db2", "tableX", null), "id"))));
        assertTrue(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db2", "tableX", "partitionX"), "id"))));
    }

    @Test()
    public void testNotNeedsCache()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -11);
        assertFalse(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table1", sdf.format(calendar.getTime())), "id"))));
        assertFalse(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table4", "p1"), "id"))));
        assertFalse(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table2", "2021-03-01"), "id"))));
        assertFalse(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("db1", "table2", "p3"), "id"))));
        assertFalse(filter.needsCache(new URIStatus(new FileInfo(), getCacheContext(new HiveCacheContext("dbX", "table2", "p1"), "id"))));
    }

    private static String getResourceFilePath(String fileName)
    {
        return TestFileBasedDateCacheFilter.class.getClassLoader().getResource(fileName).getPath();
    }

    private CacheContext getCacheContext(HiveCacheContext hiveCacheContext, String id)
    {
        CacheContext context = CacheContext.defaults();
        context.setHiveCacheContext(hiveCacheContext);
        context.setCacheIdentifier(id);
        return context;
    }
}
