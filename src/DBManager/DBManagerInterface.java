package DBManager;

import java.io.IOException;

public interface DBManagerInterface {
    public void CreateDB(String dbName) throws Exception;
    public void DropDB(String dbName) throws Exception;
    public void SelectDB(String dbName) throws Exception;
    public void UpdateTableDB(String databaseName, String tableName, char tableUpdateType) throws Exception;
}
