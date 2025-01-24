package DBManager;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DBManager implements DBManagerInterface {
    private final String dbPath = "C:\\NebulaDBStore\\";
    private String currentSelectedDatabase;
    private List<String> databaseNames = new ArrayList<>();

    // maps all the tables in a particular database
    private HashMap<String, List<String>> dbTableDirectory;
    private final long tableFileSize = 1024 * 1024 * 1024;

    public DBManager() {
        this.dbTableDirectory = new HashMap<>();
        try
        {
            StoreDatabaseNames();
            for(String databaseName : this.databaseNames){
                if(!this.dbTableDirectory.containsKey(databaseName)){
                    List<String> tableNames = GetTableNamesFromDB(dbPath.concat("\\").concat(databaseName));
                    this.dbTableDirectory.put(databaseName, tableNames);
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private List<String> GetTableNamesFromDB(String dbPath) throws Exception{
        Path path = Paths.get(dbPath);
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);
        List<String> tableNames = new ArrayList<>();
        for(Path file : directoryStream){
            if(Files.isRegularFile(file)){
                String fileName = file.getFileName().toString();
                tableNames.add(fileName);
            }
        }
        return tableNames;
    }
    private void StoreDatabaseNames() throws IOException {
        Path dirPath = Paths.get(dbPath);
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dirPath);
        for (Path dirpath : directoryStream) {
            if(Files.isDirectory(dirpath)) {
                databaseNames.add(dirpath.getFileName().toString());
            }
        }
    }

    @Override
    public void CreateDB(String dbName) throws Exception{
        if(!this.dbTableDirectory.containsKey(dbName)){
            this.dbTableDirectory.put(dbName, new ArrayList<>());
            String dirPath = dbPath.concat("\\").concat(dbName);
            Path path = Paths.get(dirPath);
            Files.createDirectories(path);
            return;
        }
        throw new Exception("Database "+dbName+" already exists");
    }

    @Override
    public void DropDB(String dbName) throws Exception{
        if(dbTableDirectory.containsKey(dbName)){
            String dirPath = dbPath.concat("\\").concat(dbName);
            dbTableDirectory.remove(dbName);
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(dirPath));
            for (Path file : directoryStream) {
                if(Files.isRegularFile(file)){
                    String fileName = file.getFileName().toString();
                    Files.delete(file);
                }
            }
            Files.delete(Paths.get(dirPath));
            return;
        }
        throw new Exception("Database" + dbName + " does not exist");
    }

    @Override
    public void SelectDB(String dbname) throws Exception{
        if(dbTableDirectory.containsKey(dbname)){
            String dirPath = dbPath.concat("\\").concat(dbname);
            currentSelectedDatabase = dbname;
            return;
        }
        throw new Exception("Cannot Select DB ,Database " + dbname + " does not exist");
    }

    @Override
    public void UpdateTableDB(String tableName, String extension, char tableUpdateType) throws Exception {
        switch(tableUpdateType){
            case 'C':
                if(this.dbTableDirectory.containsKey(currentSelectedDatabase)){
                    List<String> tableNames = this.dbTableDirectory.get(currentSelectedDatabase);
                    for(String tblName : tableNames){
                        if(tblName.equals(tableName)){
                            throw new Exception("Table Name already exists");
                        }
                    }
                    String tablePath = dbPath.concat("\\").concat(currentSelectedDatabase).concat("\\").concat(tableName).concat(extension);
                    Files.createFile(Paths.get(tablePath));
                    tableNames.add(tableName);
                    dbTableDirectory.put(currentSelectedDatabase, tableNames);
                }
                break;
            case 'D':
                if(this.dbTableDirectory.containsKey(currentSelectedDatabase)){
                    List<String> tableNames = this.dbTableDirectory.get(currentSelectedDatabase);
                    // using iterator as concurrent modification is not supported while directly iterating over the list
                    Iterator<String> iterator = tableNames.iterator();
                    while(iterator.hasNext()){
                        String tblName = iterator.next();
                        if(tblName.equals(tableName)){
                            tableNames.remove(tableName);
                            String tablePath = dbPath.concat("\\").concat(currentSelectedDatabase).concat("\\").concat(tableName).concat(extension);
                            Files.delete(Paths.get(tablePath));
                            return;
                        }
                    }
                    throw new Exception("No such table exists");
                }
                break;

                default:
                    throw new Exception("Invalid table update type");
        }
    }
}
