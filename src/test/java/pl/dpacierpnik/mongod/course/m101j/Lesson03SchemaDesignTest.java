package pl.dpacierpnik.mongod.course.m101j;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Test;

import java.util.Collection;

/**
 * Created by kamil on 06.04.14.
 */
public class Lesson03SchemaDesignTest extends BaseLessonTest {

    public Lesson03SchemaDesignTest() {
        super("school", "students", false);
    }

    @Test
    public void homework_3_1() {
// remove the grade of type "homework" with the lowest score for each student
// from the dataset that you imported in HW 2.1. Since each document is one grade,
// it should remove one document per student.

        DBObject item;
        DBObject itemQuery;
        DBObject pullOperation;

        try (DBCursor dbCursor = getDbCollection().find()) {

            while (dbCursor.hasNext()) {

                item = dbCursor.next();
                System.out.printf("=== Processing: %s\n", item);

                Collection<DBObject> scores = (Collection<DBObject>) item.get("scores");
                DBObject scoreToPull = getScoreToPull(scores);
                System.out.println("Score to pull: " + scoreToPull);

                if (scoreToPull != null) {

                    itemQuery = new BasicDBObject("_id", item.get("_id"));
                    System.out.println("Item query: " + itemQuery);

                    pullOperation = new BasicDBObject("$pull", new BasicDBObject("scores", scoreToPull));
                    System.out.println("Pull operation: " + pullOperation);

                    getDbCollection().update(itemQuery, pullOperation);
                }
                System.out.println("=======================================");
            }
        }
    }

    private DBObject getScoreToPull(Collection<DBObject> scores) {

        DBObject lowestHomeworkScore = null;
        for (DBObject score : scores) {

            if ("homework".equals(score.get("type"))) {

                if (lowestHomeworkScore == null) {

                    lowestHomeworkScore = score;
                }
                else {

                    Double lowestHomeworkScoreValue = (Double) lowestHomeworkScore.get("score");
                    Double scoreValue = (Double) score.get("score");
                    if (scoreValue.compareTo(lowestHomeworkScoreValue) == -1) {
                        lowestHomeworkScore = score;
                    }
                }
            }
        }
        return lowestHomeworkScore;
    }
}
