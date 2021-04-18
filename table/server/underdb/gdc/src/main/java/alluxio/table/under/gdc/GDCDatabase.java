package alluxio.table.under.gdc;

import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class GDCDatabase implements UnderDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(GDCDatabase.class);

    private final UdbContext mUdbContext;
    private final UdbConfiguration mConfiguration;
    private final String mGDCDbName;

    @VisibleForTesting
    protected GDCDatabase(UdbContext udbContext, UdbConfiguration GDCConfig, String GDCDbName) {
        mUdbContext = udbContext;
        mConfiguration = GDCConfig;
        mGDCDbName = GDCDbName;
    }

    public static GDCDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
        String GDCDbName = udbContext.getUdbDbName();
        return new GDCDatabase(udbContext, configuration, GDCDbName);
    }


    @Override
    public String getType() {
        return GDCDatabaseFactory.TYPE;
    }

    @Override
    public String getName() {
        return mGDCDbName;
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
        return mUdbContext;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() throws IOException {
        return null;
    }
}
