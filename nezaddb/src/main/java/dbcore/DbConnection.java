package dbcore;

import java.sql.*; //import java.util.HashMap;

import configuration.Configuration;

public class DbConnection {

    private static String conString = "jdbc:mysql://localhost/";
    private static String user = Configuration.getInstance().getOption(
            "SiteConfiguration.DatabaseConfiguration.user");
    private static String passwd = Configuration.getInstance().getOption(
            "SiteConfiguration.DatabaseConfiguration.pasw");

    // private static HashMap<String, Connection> connPool = new HashMap<String,
    // Connection>();

    /*
     * private DbConnection() {}
     */

    public static Connection getDBConnection(String database) {
        Connection conn = null;
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
            conn = DriverManager.getConnection(conString + database, user,
                    passwd);
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
        return conn;
    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null)
                conn.close();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }

    }

}
