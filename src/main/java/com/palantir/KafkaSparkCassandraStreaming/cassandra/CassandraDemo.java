package com.palantir.KafkaSparkCassandraStreaming.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraDemo {
    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint("127.0.0.1").build();
        session = cluster.connect("demo");

        String createUse = "CREATE TABLE IF NOT EXISTS persons(lastname text PRIMARY KEY,age int, city text,email text, firstname text)";
        session.execute(createUse);

        // Insert one record into the persons table
        session.execute("INSERT INTO persons (lastname, age, city, email, firstname) VALUES ('sam', 27, 'Austin', 'sam@example.com', 'sam')");

        // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM persons WHERE lastname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
        }

        // Update the same user with a new age
        session.execute("update persons set age = 36 where lastname = 'Jones'");

        // Select and show the change
        results = session.execute("select * from persons where lastname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
        }

        // Delete the user from the persons table
        //session.execute("DELETE FROM persons WHERE lastname = 'Jones'");

        // Show that the user is gone
        results = session.execute("SELECT * FROM persons");
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"),  row.getString("city"), row.getString("email"), row.getString("firstname"));
        }

        // Clean up the connection by closing it
        cluster.close();

    }
}
