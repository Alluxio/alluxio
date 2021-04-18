package alluxio.table.under.gdc;

import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import java.io.IOException;
import java.util.List;

public class GDCDatabase implements UnderDatabase {
    @Override
    public String getType() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> getTableNames() throws IOException {
        return null;
    }

    @Override
    public UdbTable getTable(String tableName) throws IOException {
        return null;
    }

    @Override
    public UdbContext getUdbContext() {
        return null;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() throws IOException {
        return null;
    }
}
