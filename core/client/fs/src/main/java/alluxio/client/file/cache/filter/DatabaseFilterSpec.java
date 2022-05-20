package alluxio.client.file.cache.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DatabaseFilterSpec
{
    private final String name;
    private final List<TableFilterSpec> tables;

    @JsonCreator
    public DatabaseFilterSpec(
            @JsonProperty("name") String databaseName,
            @JsonProperty("tables") List<TableFilterSpec> tables)
    {
        this.name = requireNonNull(databaseName, "tableName is null");
        this.tables = requireNonNull(tables, "tables is null");
    }

    public String getName()
    {
        return name;
    }

    public List<TableFilterSpec> getTables()
    {
        return tables;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof DatabaseFilterSpec)) {
            return false;
        }
        DatabaseFilterSpec that = (DatabaseFilterSpec) other;
        return name.equals(that.name) && tables.equals(that.tables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, tables);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("tables", tables)
                .toString();
    }
}
