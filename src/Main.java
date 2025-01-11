import DBManager.DBManager;

public class Main {
    public static void main(String[] args) {
        DBManager dbManager = new DBManager();
        try {
            dbManager.CreateDB("myDB");
            dbManager.SelectDB("myDB");
            dbManager.UpdateTableDB("Employees",".ntb",'C');
            dbManager.UpdateTableDB("Employees",".ntb",'D');
            dbManager.DropDB("myDB");
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
