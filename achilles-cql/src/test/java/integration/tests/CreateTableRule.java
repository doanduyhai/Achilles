package integration.tests;

import info.archinnov.achilles.common.CQLCassandraDaoTest;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import com.datastax.driver.core.Session;

public class CreateTableRule implements TestRule {

    static {
        Session session = CQLCassandraDaoTest.getCqlSession();
        createTables(session);
    }

    private static void createTables(Session session) {
        String query = "create table cql3_user(id bigint,firstname text,lastname text, age int, primary key(id))";
        session.execute(query);
    }

    @Override
    public Statement apply(Statement base, Description description) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };
    }

}
