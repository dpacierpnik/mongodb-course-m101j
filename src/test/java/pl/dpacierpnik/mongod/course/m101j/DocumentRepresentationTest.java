package pl.dpacierpnik.mongod.course.m101j;

import com.mongodb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;


public class DocumentRepresentationTest
{
	private DBCollection dbCollection;

	@Before
	public void beforeEveryTest() throws Exception
	{
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("course");
		dbCollection = db.getCollection("crudTest");
		dbCollection.drop();
	}

	@After
	public void afterEveryTest()
	{
		dbCollection.drop();
	}

	@Test
	public void insert()
	{
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

		dbCollection.insert(doc, doc2);

		System.out.println("DOC: " + doc);

		doc.removeField("_id");
		dbCollection.insert(doc);

		System.out.println("DOC: " + doc);
	}

	@Test
	public void find()
	{
		Random rand = new Random();
		for (int i = 0; i < 10; ++i)
		{
			dbCollection.insert(new BasicDBObject("x", rand.nextInt(100)));
		}
		System.out.println("\nFind one: " + dbCollection.findOne());

		System.out.println("\nFind all: ");
		try (DBCursor dbCursor = dbCollection.find())
		{
			while (dbCursor.hasNext())
			{
				System.out.println("\tItem: " + dbCursor.next());
			}
		}

		System.out.println("\nCount: " + dbCollection.count());
	}

	@Test
	public void find_queryCriteria()
	{
		Random rand = new Random();
		for (int i = 0; i < 10; ++i)
		{
			dbCollection.insert(new BasicDBObject("x", rand.nextInt(2)).append("y", rand.nextInt(100)));
		}

		//		DBObject query = new BasicDBObject("x", 0).append("y", new BasicDBObject("$gt", 10).append("$lt", 90));
		QueryBuilder queryBuilder = QueryBuilder.start("x").is(0).and("y").greaterThan(10).lessThan(90);
		DBObject query = queryBuilder.get();

		System.out.println("\nCount: " + dbCollection.count(query));

		System.out.println("\nFind all: ");
		try (DBCursor dbCursor = dbCollection.find(query))
		{
			while (dbCursor.hasNext())
			{
				System.out.println("\tItem: " + dbCursor.next());
			}
		}
	}


	@Test
	public void find_fieldSelection()
	{
		Random rand = new Random();
		for (int i = 0; i < 10; ++i)
		{
			dbCollection.insert(new BasicDBObject("x", rand.nextInt(2)).append("y", rand.nextInt(100)).append("z", rand.nextInt(1000)));
		}

		DBObject query = QueryBuilder.start("x").is(0).and("y").greaterThan(10).lessThan(70).get();
		DBObject fieldSelector = new BasicDBObject("x", false).append("_id", false);

		try (DBCursor dbCursor = dbCollection.find(query, fieldSelector))
		{
			while (dbCursor.hasNext())
			{
				DBObject dbObject = dbCursor.next();
				System.out.println("\tItem: " + dbObject);
			}
		}
	}
}
