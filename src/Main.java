import DBManager.DBManager;

public class Main {
    public static void main(String[] args) {
        DBManager dbManager = new DBManager();
        try {
            dbManager.CreateDB("myDB");
        }
        catch (Exception e) {
            System.err.println(e);
        }

        try {
            dbManager.UpdateTableDB("myDB","Employees.ntb",'C');
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
