package alluxio.table.under.gdc;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.common.udb.UnderDatabaseFactory;

public class GDCDatabaseFactory implements UnderDatabaseFactory {
    public static final String TYPE = "gdc";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public UnderDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
        return GDCDatabase.create(udbContext, configuration);
    }
}
