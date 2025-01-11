package DBManager;

import java.io.IOException;

public interface DBManagerInterface {
    public void CreateDB(String dbName) throws Exception;
    public void DropDB(String dbName) throws Exception;
    public void SelectDB(String dbName) throws Exception;
    public void UpdateTableDB(String tableName, String ext, char tableUpdateType) throws Exception;
}
