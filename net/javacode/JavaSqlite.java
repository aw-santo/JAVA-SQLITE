package net.javacode;

import java.sql.*;

import java.lang.String;

public class JavaSqlite {

    public Connection connectTo(String db_name) {
        Connection conn = null;
        try {
            // db parameters
            String url = "jdbc:sqlite:db/" + db_name;
            // create a connection to the database
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to " + db_name + " has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public void createDB(String db_name) {

//        String url = "jdbc:sqlite:db/" + db_name;

        try (Connection conn = this.connectTo(db_name)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database " + db_name +" has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void createTable(String table_name) {
        // SQLite connection string
//        String url = "jdbc:sqlite:db/test.db";

        // SQL statement for creating a new table
        String query = "CREATE TABLE IF NOT EXISTS Movies (\n"
                + "movie_name TINYTEXT PRIMARY KEY,\n"
                + "actor TINYTEXT NOT NULL,\n"
                + "actress TINYTEXT NOT NULL,\n"
                + "year_of_release INTEGER NOT NULL,\n"
                + "director_name TINYTEXT NOT NULL\n"
                + ");";

        try (Connection conn = this.connectTo("test.db");
             Statement stmt = conn.createStatement()) {
            // create a new table
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("The driver name is " + meta.getDriverName());
            System.out.println("ok");
            stmt.execute(query);
            System.out.println("Querry executed!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertInto(String table, String[] movie_data) {
//        String query = "INSERT INTO "+ table +"(movie_name, actor, actress, year_of_release, director_name) VALUES(\n"
//                + movie_data[0]+",\n"
//                + movie_data[1]+",\n"
//                + movie_data[2]+",\n"
//                + Integer.parseInt(movie_data[3])+",\n"
//                + movie_data[4]+"\n"
//                + ");";

        String query = "INSERT INTO "+table+"(movie_name,actor,actress,year_of_release,director_name) VALUES(?,?,?,?,?)";

        try (Connection conn = this.connectTo("test.db");
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            System.out.println("ok");
            pstmt.setString(1, movie_data[0]);
            pstmt.setString(2, movie_data[1]);
            pstmt.setString(3, movie_data[2]);
            pstmt.setInt(4, Integer.parseInt(movie_data[3]));
            pstmt.setString(5, movie_data[4]);
            System.out.println("okok");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void select(String table){
        this.select(table, null);
    }

    public void select(String table, String actor){

        String sql = actor==null ? "SELECT * FROM "+table : "SELECT * FROM "+table+" WHERE actor='"+actor+"';";

        try (Connection conn = this.connectTo("test.db");
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getString("movie_name") +  "\t" +
                        rs.getString("actor") + "\t" +
                        rs.getString("actress") + "\t" +
                        rs.getString("director_name") + "\t" +
                        rs.getInt("year_of_release")
                );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {

        JavaSqlite db_obj = new JavaSqlite();

        // connecting to existing Db
//        db_obj.connectTo("chinook.db");


        // creating new DB 'test.db'
//        db_obj.createDB("test.db"); // this will automatically create as well as establish the connection implicitly.
        // connecting to 'test.db'
//        db_obj.connectTo("test.db"); // no need to connect to DB explicitly.

        // creating new table 'Movies'
//        db_obj.createTable("Movies");

        // inserting data into 'Movies' table
//        String[] movies_data = {"Robot", "Rajni", "Aishwarya", "2010", "Shankar"};
//        db_obj.insertInto("Movies", movies_data);

        // selecting data
        db_obj.select("Movies");
    }
}