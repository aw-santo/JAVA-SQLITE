package net.javacode;

import java.sql.*;

public class JavaSqlite {

    public static void createDB(String db_name) {

        String url = "jdbc:sqlite:db/" + db_name;

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database " + db_name +" has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void connect(String db_name) {
        Connection conn = null;
        try {
            // db parameters
            String url = "jdbc:sqlite:db/" + db_name;
            // create a connection to the database
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to " + db_name + " has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
    public static void main(String[] args) {

        connect("chinook.db");

        String data_base = "test.db";
        createDB(data_base);
        connect(data_base);
    }
}