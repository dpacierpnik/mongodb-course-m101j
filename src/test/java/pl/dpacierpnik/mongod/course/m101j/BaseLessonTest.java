package pl.dpacierpnik.mongod.course.m101j;

import com.mongodb.*;
import org.junit.After;
import org.junit.Before;

/**
 * Created by kamil on 06.04.14.
 */
public class BaseLessonTest {

    private String dbName;
    private String collectionName;
    private boolean autoCleanup;

    private DBCollection dbCollection;

    public BaseLessonTest(String collectionName) {
        this("course", collectionName, true);
    }

    public BaseLessonTest(String dbName, String collectionName, boolean autoCleanup) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.autoCleanup = autoCleanup;
    }

    @Before
    public void beforeEveryTest() throws Exception {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(dbName);
        dbCollection = db.getCollection(collectionName);
        if (autoCleanup) {
            dbCollection.drop();
        }
    }

    @After
    public void afterEveryTest() {
        if (autoCleanup) {
            dbCollection.drop();
        }
    }

    protected DBCollection getDbCollection() {
        return dbCollection;
    }

    protected void printDbCollection(DBCollection dbCollection) {
        System.out.println("======================");
        try (DBCursor dbCursor = dbCollection.find()) {
            printDbCursorItems(dbCursor);
        }
    }


    protected void findAndPrintDbCollection(DBObject query) {
        findAndPrintDbCollection(dbCollection, query);
    }

    protected void findAndPrintDbCollection(DBCollection dbCollection, DBObject query) {
        System.out.println("Query: '" + query + "' ======================");
        try (DBCursor dbCursor = dbCollection.find(query)) {
            printDbCursorItems(dbCursor);
        }
    }

    protected void printDbCursorItems(DBCursor dbCursor) {
        while (dbCursor.hasNext()) {
            System.out.println("\tItem: " + dbCursor.next());
        }
    }
}
