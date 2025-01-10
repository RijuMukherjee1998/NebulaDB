package StorageEngine;

public class PageHeader {
    final int pageSize = 4 * 1024;
    final byte dbmsVersion;
    String[] schemaInfo;
    byte[] checksum;
    boolean pageModifiability;
    public PageHeader(byte dbmsVersion, String[] schemaInfo, boolean pageModifiability) {
        this.dbmsVersion = dbmsVersion;
        this.schemaInfo = schemaInfo;
        this.pageModifiability = pageModifiability;
    }
}
