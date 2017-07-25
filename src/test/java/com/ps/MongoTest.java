package com.ps;

import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.mongodb.client.model.Filters.*;

public class MongoTest {

    private MongoClient mongo;
    private MongoDatabase db;

    @Before
    public void setUp() throws InterruptedException {
        mongo = MongoClients.create(new ConnectionString("mongodb://localhost:27017"));
        db = mongo.getDatabase("test");
        MongoCollection<Document> users = db.getCollection("users");
        CountDownLatch done = new CountDownLatch(3);
        users.insertOne(new Document()
                .append("_id", new ObjectId())
                .append("firstName", "pavlo")
                .append("lastName", "pohrebnyi")
                .append("age", 27)
                .append("address", new Document()
                        .append("city", "Kiyv")
                        .append("street", "xxx")), (r, t) -> {
            done.countDown();
        });
        users.insertOne(new Document()
                .append("_id", new ObjectId())
                .append("firstName", "randy")
                .append("lastName", "baiad")
                .append("age", 45)
                .append("address", new Document()
                        .append("city", "Rye")
                        .append("street", "xxx")), (r, t) -> {
            done.countDown();
        });
        users.createIndex(
                Indexes.ascending("firstName", "lastName"),
                (result, t) -> done.countDown());
        done.await();
    }

    @After
    public void tearDown() {
        db.drop((r, t) -> mongo.close());
    }

    @Test
    public void queryAll() throws InterruptedException {
        MongoCollection<Document> users = db.getCollection("users");
        CountDownLatch done = new CountDownLatch(1);
        users.find().forEach(
                document -> System.out.println(document.toJson()),
                (result, t) -> done.countDown());
        done.await();
    }

    @Test
    public void queryWithFilter() throws InterruptedException {
        MongoCollection<Document> users = db.getCollection("users");
        CountDownLatch done = new CountDownLatch(1);
        users.find(eq("firstName", "pavlo")).forEach(
                document -> System.out.println(document.toJson()),
                (result, t) -> done.countDown());
        done.await();
    }

    @Test
    public void update() throws InterruptedException {
        MongoCollection<Document> users = db.getCollection("users");
        CountDownLatch done = new CountDownLatch(1);
        users.updateMany(eq("firstName", "pavlo"),
                        new Document("$set", new Document("lastName", "Man")),
                        (result, t) -> done.countDown());
        done.await();
    }

    @Test
    public void delete() throws InterruptedException {
        MongoCollection<Document> users = db.getCollection("users");
        CountDownLatch done = new CountDownLatch(1);
        users.deleteMany(eq("firstName", "pavlo"),
                (result, t) -> done.countDown());
        done.await();
    }

}
