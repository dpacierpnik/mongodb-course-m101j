package pl.dpacierpnik.mongod.course.m101j;

import com.mongodb.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class Lesson02CrudTest extends BaseLessonTest {

    public Lesson02CrudTest() {
        super("lesson02CrudTest");
    }

    @Test
    public void insert() {
        BasicDBObject doc = new BasicDBObject();
        doc.put("username", "dpacierpnik");
        doc.put("cdate", new Date());
        doc.put("age", 31);
        doc.put("programmer", true);
        doc.put("languages", Arrays.asList("Java", "Scala"));
        doc.put("address", new BasicDBObject("street", "Some Street 1").append("Town", "Some Town 1"));

        BasicDBObject doc2 = new BasicDBObject("_id", "test");

        System.out.println("DOC: " + doc);
        System.out.println("DOC2: " + doc2);

        getDbCollection().insert(doc, doc2);

        System.out.println("DOC: " + doc);

        doc.removeField("_id");
        getDbCollection().insert(doc);

        System.out.println("DOC: " + doc);
    }

    @Test
    public void find() {
        Random rand = new Random();
        for (int i = 0; i < 10; ++i) {
            getDbCollection().insert(new BasicDBObject("x", rand.nextInt(100)));
        }
        System.out.println("\nFind one: " + getDbCollection().findOne());

        System.out.println("\nFind all: ");
        try (DBCursor dbCursor = getDbCollection().find()) {
            printDbCursorItems(dbCursor);
        }

        System.out.println("\nCount: " + getDbCollection().count());
    }

    @Test
    public void find_queryCriteria() {
        Random rand = new Random();
        for (int i = 0; i < 10; ++i) {
            getDbCollection().insert(new BasicDBObject("x", rand.nextInt(2)).append("y", rand.nextInt(100)));
        }

        //		DBObject query = new BasicDBObject("x", 0).append("y", new BasicDBObject("$gt", 10).append("$lt", 90));
        QueryBuilder queryBuilder = QueryBuilder.start("x").is(0).and("y").greaterThan(10).lessThan(90);
        DBObject query = queryBuilder.get();

        System.out.println("\nCount: " + getDbCollection().count(query));

        System.out.println("\nFind all: ");
        try (DBCursor dbCursor = getDbCollection().find(query)) {
            printDbCursorItems(dbCursor);
        }
    }


    @Test
    public void find_fieldSelection() {
        Random rand = new Random();
        for (int i = 0; i < 10; ++i) {
            getDbCollection().insert(new BasicDBObject("x", rand.nextInt(2)).append("y", rand.nextInt(100)).append("z", rand.nextInt(1000)));
        }

        DBObject query = QueryBuilder.start("x").is(0).and("y").greaterThan(10).lessThan(70).get();
        DBObject fieldSelector = new BasicDBObject("x", false).append("_id", false);

        try (DBCursor dbCursor = getDbCollection().find(query, fieldSelector)) {
            printDbCursorItems(dbCursor);
        }
    }

    @Test
    public void dotNotation() {
        Random rand = new Random();
        for (int i = 0; i < 10; ++i) {
            getDbCollection().insert(new BasicDBObject("_id", i)
                    .append("start",
                            new BasicDBObject("x", rand.nextInt(90) + 10)
                                    .append("y", rand.nextInt(90) + 10))
                    .append("end",
                            new BasicDBObject("x", rand.nextInt(90) + 10)
                                    .append("y", rand.nextInt(90) + 10))
            );
        }

        QueryBuilder builder = QueryBuilder.start("start.x").greaterThan(50);

        try (DBCursor dbCursor = getDbCollection().find(builder.get(), new BasicDBObject("start.y", true).append("_id", false))) {
            printDbCursorItems(dbCursor);
        }
    }

    @Test
    public void updateAndRemove() {

        List<String> names = Arrays.asList("alice", "bobby", "cathy", "david", "ethan");
        for (String name : names) {
            getDbCollection().insert(new BasicDBObject("_id", name));
        }
        printDbCollection(getDbCollection());

        getDbCollection().update(new BasicDBObject("_id", "alice"), new BasicDBObject("age", 24));
        printDbCollection(getDbCollection());

        getDbCollection().update(new BasicDBObject("_id", "alice"), new BasicDBObject("$set", new BasicDBObject("gender", "F")));
        printDbCollection(getDbCollection());

        getDbCollection().update(new BasicDBObject("_id", "frank"), new BasicDBObject("$set", new BasicDBObject("gender", "M")), true, false);
        printDbCollection(getDbCollection());

        getDbCollection().update(new BasicDBObject(), new BasicDBObject("$set", new BasicDBObject("title", "Dr")), false, true);
        printDbCollection(getDbCollection());

        getDbCollection().remove(new BasicDBObject("_id", "alice"));
        printDbCollection(getDbCollection());
    }

    @Test
    public void findAndModify() {

        final String counterId = "abc";
        int first;
        int numNeeded;

        numNeeded = 2;
        first = getRange(counterId, numNeeded, getDbCollection());
        printRangeInfo(first, numNeeded);

        numNeeded = 3;
        first = getRange(counterId, numNeeded, getDbCollection());
        printRangeInfo(first, numNeeded);

        numNeeded = 10;
        first = getRange(counterId, numNeeded, getDbCollection());
        printRangeInfo(first, numNeeded);
    }

    private int getRange(String id, int range, DBCollection dbCollection) {
        DBObject doc = dbCollection.findAndModify(new BasicDBObject("_id", id), null, null, false, new BasicDBObject("$inc", new BasicDBObject("counter", range)), true, true);
        return (Integer)doc.get("counter") - range + 1;
    }

    private void printRangeInfo(int first, int numNeeded) {
        System.out.println("Range: " + first + "-" + (first + numNeeded - 1));
    }
}
