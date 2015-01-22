package info.archinnov.achilles.script;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.query.cql.NativeQueryValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DDL_SCRIPT;
import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DML_STATEMENT;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Facility class to execute a CQL script file or a plain CQL statement
 */
public class ScriptExecutor {

    private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    private static final Logger DDL_LOGGER = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

    private static final String COMMA = ";";
    private static final String BATCH_BEGIN = "BEGIN";
    private static final String BATCH_APPLY = "APPLY";

    private final Session session;

    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    private NativeQueryValidator queryValidator = NativeQueryValidator.Singleton.INSTANCE.get();

    public ScriptExecutor(Session session) {
        this.session = session;
    }

    /**
     * Execute a CQL script file located in the class path
     * @param scriptLocation
     *      the location of the script file in the class path
     */
    public void executeScript(String scriptLocation) {
        final List<SimpleStatement> statements = buildStatements(loadScriptAsLines(scriptLocation));
        for (SimpleStatement statement : statements) {
            if (queryValidator.isDMLStatement(statement)) {
                DML_LOGGER.debug("\tSCRIPT : {}", statement.getQueryString());
            } else {
                DDL_LOGGER.debug("\t\tSCRIPT : {}", statement.getQueryString());
            }
            session.execute(statement);
        }
    }

    /**
     * Execute a plain CQL string statement
     * @param statement
     *      plain CQL string statement
     *
     * @return the resultSet
     *
     */
    public ResultSet execute(String statement) {
        return session.execute(statement);
    }

    /**
     * Execute a CQL statement
     * @param statement
     *      CQL statement
     *
     * @return the resultSet
     *
     */
    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    /**
     * Execute a plain CQL string statement asynchronously
     *
     * @param statement the CQL string statement
     *
     * @return AchillesFuture&lt;ResultSet&gt;
     */
    public AchillesFuture<ResultSet> executeAsync(String statement) {
        return asyncUtils.buildInterruptible(session.executeAsync(statement));
    }

    /**
     * Execute a CQL statement asynchronously
     *
     * @param statement CQL statement
     *
     * @return AchillesFuture&lt;ResultSet&gt;
     */
    public AchillesFuture<ResultSet> executeAsync(Statement statement) {
        return asyncUtils.buildInterruptible(session.executeAsync(statement));
    }

    protected List<String> loadScriptAsLines(String scriptLocation) {

        InputStream inputStream = this.getClass().getResourceAsStream("/" + scriptLocation);

        Validator.validateNotNull(inputStream,"Cannot find CQL script file at location '%s'", scriptLocation);

        Scanner scanner = new Scanner(inputStream);
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            final String nextLine = scanner.nextLine().trim();
            if (isNotBlank(nextLine)) {
                lines.add(nextLine);
            }
        }
        return lines;
    }

    protected List<SimpleStatement> buildStatements(List<String> lines) {
        List<SimpleStatement> statements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        boolean batch = false;
        StringBuilder batchStatement = new StringBuilder();
        for (String line : lines) {
            if (line.trim().startsWith(BATCH_BEGIN)) {
                batch = true;
            }
            if (batch) {
                batchStatement.append(line);
                if (line.startsWith(BATCH_APPLY)) {
                    batch = false;
                    statements.add(new SimpleStatement(batchStatement.toString()));
                    batchStatement = new StringBuilder();
                }
            } else {
                statement.append(line);
                if (line.endsWith(COMMA)) {
                    statements.add(new SimpleStatement(statement.toString()));
                    statement = new StringBuilder();
                } else {
                    statement.append(" ");
                }
            }

        }
        return statements;
    }
}
