package pl.dpacierpnik.mongod.course.m101j;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Test;

/**
 * Created by kamil on 06.04.14.
 */
public class Lesson02CrudHomeworkTest extends BaseLessonTest {

    public Lesson02CrudHomeworkTest() {
        super("students", "grades", false);
    }

    @Test
    public void homework_2_2() {
// remove the grade of type "homework" with the lowest score for each student
// from the dataset that you imported in HW 2.1. Since each document is one grade,
// it should remove one document per student.

        DBObject query = new BasicDBObject("type", "homework");
        DBObject sorting = new BasicDBObject("student_id", 1).append("score", 1);

        Integer lastStudentId = null;
        DBObject item;
        try (DBCursor dbCursor = getDbCollection().find(query)) {
            dbCursor.sort(sorting);
            while (dbCursor.hasNext()) {
                item = dbCursor.next();
                if (isStudentIdChanged(lastStudentId, item)) {
                    getDbCollection().remove(new BasicDBObject("_id", item.get("_id")));
                    lastStudentId = (Integer) item.get("student_id");
                }
            }
        }
    }

    private boolean isStudentIdChanged(Integer lastStudentId, DBObject item) {
        return lastStudentId == null || !lastStudentId.equals(item.get("student_id"));
    }
}
