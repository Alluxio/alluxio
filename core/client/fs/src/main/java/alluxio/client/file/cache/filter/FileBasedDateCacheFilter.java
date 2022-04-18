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

package alluxio.client.file.cache.filter;

import alluxio.client.file.URIStatus;
import alluxio.client.hive.HiveCacheContext;
import alluxio.conf.AlluxioConfiguration;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class FileBasedDateCacheFilter
        implements CacheFilter
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final Map<String, Map<String, PartitionLimit>> databaseCacheFilter;

    public FileBasedDateCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        this.databaseCacheFilter = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            FilterSpec filterSpec = objectMapper.readValue(
                    Paths.get(cacheConfigFile).toFile(), FilterSpec.class);
            for (DatabaseFilterSpec dbFilterSpec : filterSpec.getDatabases()) {
                databaseCacheFilter.computeIfAbsent(dbFilterSpec.getName(), key -> new HashMap<>());
                dbFilterSpec.getTables().forEach(
                        table -> {
                            PartitionLimit partitionLimit = new PartitionLimit(table.getMaxCachedPartitions());
                            databaseCacheFilter.get(dbFilterSpec.getName()).put(table.getName(), partitionLimit);
                        }
                );
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnrecognizedPropertyException) {
                UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                String message = format("Unknown property at line %s:%s: %s",
                        ex.getLocation().getLineNr(),
                        ex.getLocation().getColumnNr(),
                        ex.getPropertyName());
                throw new IllegalArgumentException(message, e);
            }
            if (cause instanceof JsonMappingException) {
                // remove the extra "through reference chain" message
                if (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                throw new IllegalArgumentException(cause.getMessage(), e);
            }
            throw e;
        }
    }

    private static int parseDate(String value)
    {
        return (int) TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value));
    }

    public boolean needsCache(URIStatus uriStatus)
    {
        HiveCacheContext hiveCacheContext = uriStatus.getCacheContext().getHiveCacheContext();
        if (hiveCacheContext == null) {
            return false;
        }
        String database = hiveCacheContext.getDatabase();
        String table = hiveCacheContext.getTable();
        String partition = hiveCacheContext.getPartition();

        if (!databaseCacheFilter.containsKey(database)) {
            return false;
        }
        Map<String, PartitionLimit> tableLimit = databaseCacheFilter.get(database);
        if (tableLimit.isEmpty()) {
            return true;
        }
        if (partition == null) {
            return tableLimit.containsKey(table);
        }

        if (!tableLimit.containsKey(table)) {
            return false;
        }
        PartitionLimit partitionLimit = tableLimit.get(table);
        try {
            int partitionDay = parseDate(partition);
            int currentDay = (int) TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
            return (currentDay - partitionDay) <= partitionLimit.getCount();
        }
        catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private static class PartitionLimit
    {
        private final int count;

        public PartitionLimit(int count)
        {
            this.count = count;
        }

        public int getCount()
        {
            return count;
        }
    }
}