package net.javacode;

import java.sql.*;

import java.lang.String;

public class JavaSqlite {

    private Connection connectTo(String db_name) {
        Connection conn = null;
        try {
            // DB url
            String url = "jdbc:sqlite:db/" + db_name;

            // creating a connection to the database 'db_name'
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to " + db_name + " has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    private void createDB(String db_name) {

        try (Connection conn = this.connectTo(db_name)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Driver : " + meta.getDriverName());
                System.out.println("A new database " + db_name +" has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void createTable(String table_name) {

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

            // metadata
//            DatabaseMetaData meta = conn.getMetaData();
//            System.out.println("Driver : " + meta.getDriverName());

            stmt.execute(query);
            System.out.println("Querry executed!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void insertInto(String table, String[] movie_data) {
        // SQl query
        String query = "INSERT INTO "+table+"(movie_name,actor,actress,year_of_release,director_name) VALUES(?,?,?,?,?)";

        try (Connection conn = this.connectTo("test.db");
             PreparedStatement pstmt = conn.prepareStatement(query)) {

            // setting data
            pstmt.setString(1, movie_data[0]);
            pstmt.setString(2, movie_data[1]);
            pstmt.setString(3, movie_data[2]);
            pstmt.setInt(4, Integer.parseInt(movie_data[3]));
            pstmt.setString(5, movie_data[4]);

            pstmt.executeUpdate();
            System.out.println("Data inserted");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void select(String table){
        this.select(table, null);
    }

    private void select(String table, String actor){

        String query = actor==null ? "SELECT * FROM "+table : "SELECT * FROM "+table+" WHERE actor='"+actor+"';";

        try (Connection conn = this.connectTo("test.db");
             Statement stmt  = conn.createStatement();

             // fetching rows (result-set)
             ResultSet rows    = stmt.executeQuery(query)){

            // loop through all the rows
            while (rows.next()) {
                System.out.println(rows.getString("movie_name") +  "\t" +
                        rows.getString("actor") + "\t" +
                        rows.getString("actress") + "\t" +
                        rows.getString("director_name") + "\t" +
                        rows.getInt("year_of_release")
                );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {

        JavaSqlite db_obj = new JavaSqlite();

        // connecting to existing Db
        db_obj.connectTo("chinook.db");


        // ** All the methods of db_obj are internally calling the connectTo() method so no need to call it explicitly.
        // creating new DB 'test.db'
        db_obj.createDB("test.db"); // this will automatically create as well as establish the connection implicitly.
        // connecting to 'test.db'
        db_obj.connectTo("test.db"); // no need to connect to DB explicitly.

        // creating new table 'Movies'
        db_obj.createTable("Movies");

        // inserting data into 'Movies' table
        String[][] movies_data = {{"Robot", "Rajni", "Aishwarya", "2010", "Shankar"},
                                    {"Mission: Impossible - Fallout", "Tom Cruise", "Michelle", "2018", "Christopher McQuarrie"},
                                    {"Interstellar", "Matthew McConaughey", "Anne", "2014", "Christopher Nolan"},
                                    {"Inception", "Leonardo DiCaprio", "Elliot", "2010", "Christopher Nolan"},
                                    {"The Matrix", "Keenu Reeves", "Carrie-Anne", "1999", "Lana, Lily Wachowski"},
                                    {"Baahubali: The Beginning", "Prabhas", "Tamannaah", "2015", "S. S. Rajamouli"},
                                    {"Avengers: Endgame", "Robert Downey JR", "Scarlett", "2019", "Anthony, Joe Russo"}
                                };

        for (int i = 0; i < movies_data.length; i++) {
            // insertion
            db_obj.insertInto("Movies", movies_data[i]);
        }

        // selecting data
        System.out.println("All rows");
        db_obj.select("Movies"); // all rows

        System.out.println("Rows having specific parameters");
        db_obj.select("Movies", "Tom Cruise"); // having actor name='Tom Cruise'
    }
}