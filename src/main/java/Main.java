import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:neo4j:bolt://localhost:7687";
        try (Connection con = DriverManager.getConnection(url, "neo4j", "XXXXX")) {

            // cwiczenie 1
            // Serwer zainstalowano lokalnie, uzyto Neo4j Community Edition.

            // cwiczenie 2
            // Wgrano bazę.

            // cwiczenie 3
            // findPerson(con, "Ed Harris");

            // cwiczenie 4
            // addMovie(con, "Skyfall", "null", 2012);
            // addActor(con, "Skyfall", "Daniel Craig", 1968);
            // addActor(con, "Skyfall", "Judi Dench", 1934);

            // cwiczenie 5
            // setNewAttributes(con, "Daniel Craig", "Chester", "02-03-1968");
            // setNewAttributes(con, "Judi Dench", "York", "09-01-1934");

            // cwiczenie 6
            // resetTaglinesDependingOnYear(con, 2000);

            // cwiczenie 7
            // findActorsWith2OrMoreFilms(con);

            // cwiczenie 8
            // getAvaregeOfActorsWith3OrMoreFilms(con);

            // cwiczenie 9
            // changeAttributeOnPath(con, "Julia Roberts", "Parker Posey");

            // cwiczenie 10
            // getSecondNodeFromPathOf4(con, "Julia Roberts", "The Da Vinci Code");

            // cwiczenie 11
            // compareTimes(con,"Ed Harris");

            // cwiczenie 12
            // przyspieszczam czas wykonania funkcji z cwiczenia 5 za pomoca dodania indeksu na imie
            // setNewAttributesWithIndex(con, "Daniel Craig", "Chester", "02-03-1968");
            // setNewAttributesWithIndex(con, "Judi Dench", "York", "09-01-1934");
            // przyspieszczam czas wykonania funkcji z cwiczenia 6 za pomoca dodania indeksu na rok
            // resetTaglinesDependingOnYearWithIndex(con, 2000);

            // cwiczenie 13
            createMinimalSpaningTree(con);


        }
    }

    public static void findPerson(Connection con, String name) throws SQLException {

        String sql = "MATCH (p:Person) " +
                "WHERE p.name = {1}" +
                "RETURN p.name as name, p.born as born";

        try (PreparedStatement stmt = con.prepareStatement(sql)) {
            stmt.setString(1, name);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.println("Person: " + rs.getString("name") + " Born: " + rs.getString("born"));
                }
            }
        }

    }

    public static void addMovie(Connection con, String movie, String tagline, int year) throws SQLException {

        String sqlMovie = "CREATE (m:Movie " +
                "{title: {1}, " +
                "tagline: {2}, " +
                "released: {3}})";

        try (PreparedStatement stmt = con.prepareStatement(sqlMovie)) {
            stmt.setString(1, movie);
            stmt.setString(2, tagline);
            stmt.setInt(3, year);

            ResultSet rs = stmt.executeQuery();
        }

    }

    public static void addActor(Connection con, String movie, String name, int born) throws SQLException {

        String sqlActor = "CREATE (p:Person " +
                "{name: {1}, " +
                "born: {2}})";

        try (PreparedStatement stmt = con.prepareStatement(sqlActor)) {
            stmt.setString(1, name);
            stmt.setInt(2, born);

            ResultSet rs = stmt.executeQuery();

        }

        String sqlRelation = "MATCH (p:Person), (m:Movie)" +
                "WHERE p.name = {1} AND m.title = {2}" +
                "CREATE (p)-[:ACTED_IN]->(m)";


        try (PreparedStatement stmt = con.prepareStatement(sqlRelation)) {
            stmt.setString(1, name);
            stmt.setString(2, movie);

            ResultSet rs = stmt.executeQuery();

        }

    }

    public static void setNewAttributes(Connection con, String name, String birthplace, String birthdate) throws SQLException {

        String sqlSet = "MATCH (p:Person) " +
                "WHERE p.name = {1}" +
                "SET p.birthplace = {2}, p.birthdate = {3}";

        try (PreparedStatement stmt = con.prepareStatement(sqlSet)) {
            stmt.setString(1, name);
            stmt.setString(2, birthplace);
            stmt.setString(3, birthdate);

            ResultSet rs = stmt.executeQuery();

        }

    }

    public static void resetTaglinesDependingOnYear(Connection con, int year) throws SQLException {

        String sqlReset = "MATCH (m:Movie) " +
                "WHERE m.released = {1}" +
                "SET m.tagline = \"null\"";

        try (PreparedStatement stmt = con.prepareStatement(sqlReset)) {
            stmt.setInt(1, year);

            ResultSet rs = stmt.executeQuery();

        }

    }

    public static void findActorsWith2OrMoreFilms(Connection con) throws SQLException {

        String sqlFind = "MATCH (p:Person)-[a:ACTED_IN]->(m:Movie) " +
                "WITH p, COLLECT(a) as relations " +
                "WHERE LENGTH(relations) >= 2 " +
                "RETURN p.name as name";

        try (PreparedStatement stmt = con.prepareStatement(sqlFind)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.println("Person: " + rs.getString("name"));
                }
            }
        }

    }

    public static void getAvaregeOfActorsWith3OrMoreFilms(Connection con) throws SQLException {
        String sqlAverage = "MATCH (p:Person)-[a:ACTED_IN]->(m:Movie) " +
                "WITH p, COLLECT(a) as relations " +
                "WHERE LENGTH(relations) >= 3  " +
                "RETURN AVG(LENGTH(relations))";

        try (PreparedStatement stmt = con.prepareStatement(sqlAverage)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.println("Average: " + rs.getDouble(1));
                }
            }
        }
    }


    public static void changeAttributeOnPath(Connection con, String name, String name2) throws SQLException {
        String sqlPath = "MATCH p = shortestPath((actor: Person {name: {1}})-[*]-(actor2: Person {name: {2}})) " +
                "FOREACH (x in nodes(p) | SET x.born = 'not known')";

        try (PreparedStatement stmt = con.prepareStatement(sqlPath)) {
            stmt.setString(1, name);
            stmt.setString(2, name2);

            ResultSet rs = stmt.executeQuery();

        }

    }

    public static void getSecondNodeFromPathOf4(Connection con, String name, String movie) throws SQLException {
        String sqlFour = "MATCH p = ((actor: Person {name: {1}})-[*3]-(movie: Movie {title: {2}})) " +
                "RETURN nodes(p)[1].title as title";

        try (PreparedStatement stmt = con.prepareStatement(sqlFour)) {
            stmt.setString(1, name);
            stmt.setString(2, movie);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                System.out.println("Title: " + rs.getString("title"));
            }
        }
    }

    public static void compareTimes(Connection con, String name) throws SQLException {

        String sqlIndexDrop = "DROP INDEX ON :Person(name)";

        try (PreparedStatement stmt = con.prepareStatement(sqlIndexDrop)) {

            ResultSet rs = stmt.executeQuery();

        }

        long startTime = System.nanoTime();
        findPerson(con, name);
        long endTime = System.nanoTime();

        String sqlIndexCreate = "CREATE INDEX ON :Person(name)";

        try (PreparedStatement stmt = con.prepareStatement(sqlIndexCreate)) {

            ResultSet rs = stmt.executeQuery();

        }

        long startTime1 = System.nanoTime();
        findPerson(con, name);
        long endTime1 = System.nanoTime();


        System.out.println("Execution time without indexes: " + (endTime - startTime));
        System.out.println("Execution time with indexes: "+ (endTime1 - startTime1));

    }

    public static void resetTaglinesDependingOnYearWithIndex(Connection con, int year) throws SQLException {


        String sqlIndexCreate = "CREATE INDEX ON :Movie(released)";

        try (PreparedStatement stmt = con.prepareStatement(sqlIndexCreate)) {

            ResultSet rs = stmt.executeQuery();

        }

        String sqlReset = "MATCH (m:Movie) " +
                "WHERE m.released = {1}" +
                "SET m.tagline = \"null\"";

        try (PreparedStatement stmt = con.prepareStatement(sqlReset)) {
            stmt.setInt(1, year);

            ResultSet rs = stmt.executeQuery();

        }

    }


    public static void setNewAttributesWithIndex(Connection con, String name, String birthplace, String birthdate) throws SQLException {

        String sqlIndexCreate = "CREATE INDEX ON :Person(name)";

        try (PreparedStatement stmt = con.prepareStatement(sqlIndexCreate)) {

            ResultSet rs = stmt.executeQuery();

        }

        String sqlSet = "MATCH (p:Person) " +
                "WHERE p.name = {1}" +
                "SET p.birthplace = {2}, p.birthdate = {3}";

        try (PreparedStatement stmt = con.prepareStatement(sqlSet)) {
            stmt.setString(1, name);
            stmt.setString(2, birthplace);
            stmt.setString(3, birthdate);

            ResultSet rs = stmt.executeQuery();

        }

    }

    public static void createMinimalSpaningTree(Connection con) throws SQLException {

        createGraph(con);

    }

    public static void createGraph(Connection con) throws SQLException {

        String sqlCreate = "MERGE (a:POINT{num:1})" +
                "MERGE (b:POINT{num:2})" +
                "MERGE (c:POINT{num:3})" +
                "MERGE (d:POINT{num:4})" +
                "MERGE (e:POINT{num:5})" +
                "MERGE (f:POINT{num:6})" +
                "MERGE (g:POINT{num:7})" +
                "MERGE (a)-[r1:Relation {cost: 2}]->(b)" +
                "MERGE (b)-[r2:Relation {cost: 5}]->(c)" +
                "MERGE (c)-[r3:Relation {cost: 1}]->(e)" +
                "MERGE (e)-[r4:Relation {cost: 3}]->(b)" +
                "MERGE (b)-[r5:Relation {cost: 1}]->(d)" +
                "MERGE (d)-[r6:Relation {cost: 2}]->(f)" +
                "MERGE (f)-[r7:Relation {cost: 3}]->(g)";

        try (PreparedStatement stmt = con.prepareStatement(sqlCreate)) {

            ResultSet rs = stmt.executeQuery();

        }

    }

}
