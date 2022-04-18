package alluxio.client.file.cache.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableFilterSpec
{
    private final String name;
    private final int maxCachedPartitions;

    @JsonCreator
    public TableFilterSpec(
            @JsonProperty("name") String tableName,
            @JsonProperty("maxCachedPartitions") int maxCachedPartitions)
    {
        this.name = requireNonNull(tableName, "tableName is null");
        checkArgument(maxCachedPartitions >= 0, "maxCachedPartitions is negative");
        this.maxCachedPartitions = maxCachedPartitions;
    }

    public String getName()
    {
        return name;
    }

    public int getMaxCachedPartitions()
    {
        return maxCachedPartitions;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof TableFilterSpec)) {
            return false;
        }
        TableFilterSpec that = (TableFilterSpec) other;
        return (name.equals(that.name) &&
                maxCachedPartitions == that.maxCachedPartitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, maxCachedPartitions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("maxCachedPartitions", maxCachedPartitions)
                .toString();
    }
}
