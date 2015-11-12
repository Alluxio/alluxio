package tachyon.client.table;

import tachyon.annotation.PublicApi;

@PublicApi
public class TachyonRawTables extends AbstractTachyonRawTables {
  private static TachyonRawTables sTachyonRawTables;

  public static class TachyonRawTablesFactory {
    private TachyonRawTablesFactory() {} // prevent init

    public static synchronized TachyonRawTables get() {
      if (sTachyonRawTables == null) {
        sTachyonRawTables = new TachyonRawTables();
      }
      return sTachyonRawTables;
    }
  }

  private TachyonRawTables() {
    super();
  }
}
