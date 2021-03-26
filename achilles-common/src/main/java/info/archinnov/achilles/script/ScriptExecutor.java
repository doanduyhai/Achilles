/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.script;


import static info.archinnov.achilles.internals.statement.StatementHelper.isDMLStatement;
import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DDL_SCRIPT;
import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DML_STATEMENT;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.MoreExecutors;

import info.archinnov.achilles.internals.futures.FutureUtils;
import info.archinnov.achilles.validation.Validator;

/**
 * Facility class to execute a CQL script file or a plain CQL statement
 */
public class ScriptExecutor {

    private static final Logger DML_LOGGER = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    private static final Logger DDL_LOGGER = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

    private static final String COMMA = ";";
    private static final String BATCH_BEGIN = "BEGIN";
    private static final String BATCH_APPLY = "APPLY";

    private static final String CODE_DELIMITER_START = "^\\s*(?:AS)?\\s*\\$\\$\\s*$";
    private static final String CODE_DELIMITER_END = "^\\s*\\$\\$\\s*;\\s*$";
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-z][a-zA-Z0-9_]*)\\}");
    private static final Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[\\{\\}\\(\\)\\[\\]\\.\\+\\*\\?\\^\\$\\\\\\|]");

    private static final Map<String, Object> EMPTY_MAP = new HashMap<>();

    private final ExecutorService sameThreadExecutor = MoreExecutors.newDirectExecutorService();
    private final Session session;

    public ScriptExecutor(Session session) {
        this.session = session;
    }

    /**
     * Execute a CQL script file located in the class path
     *
     * @param scriptLocation the location of the script file in the class path
     */
    public void executeScript(String scriptLocation) {
        executeScriptTemplate(scriptLocation, EMPTY_MAP);
    }

    /**
     * Execute a CQL script template located in the class path and
     * inject provided values into the template to produce the actual script
     *
     * @param scriptTemplateLocation the location of the script template in the class path
     * @param values                 template values
     */
    public void executeScriptTemplate(String scriptTemplateLocation, Map<String, Object> values) {
        final List<SimpleStatement> statements = buildStatements(loadScriptAsLines(scriptTemplateLocation, values));
        for (SimpleStatement statement : statements) {
            if (isDMLStatement(statement)) {
                DML_LOGGER.debug("\tSCRIPT : {}\n", statement.getQueryString());
            } else {
                DDL_LOGGER.debug("\tSCRIPT : {}\n", statement.getQueryString());
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
     * @return CompletableFuture&lt;ResultSet&gt;
     */
    public CompletableFuture<ResultSet> executeAsync(String statement) {
        return FutureUtils.toCompletableFuture(session.executeAsync(statement), sameThreadExecutor);
    }

    /**
     * Execute a CQL statement asynchronously
     *
     * @param statement CQL statement
     * @return CompletableFuture&lt;ResultSet&gt;
     */
    public CompletableFuture<ResultSet> executeAsync(Statement statement) {
        return FutureUtils.toCompletableFuture(session.executeAsync(statement), sameThreadExecutor);
    }

    protected List<String> loadScriptAsLines(String scriptLocation) {
        return loadScriptAsLines(scriptLocation, EMPTY_MAP);
    }

    protected List<String> loadScriptAsLines(String scriptLocation, Map<String, Object> variables) {

        InputStream inputStream = this.getClass().getResourceAsStream("/" + scriptLocation);

        Validator.validateNotNull(inputStream, "Cannot find CQL script file at location '%s'", scriptLocation);

        Scanner scanner = new Scanner(inputStream);
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String nextLine = maybeReplaceVariables(scanner, variables);
            if (isNotBlank(nextLine)) {
                lines.add(nextLine);
            }
        }
        return lines;
    }

    private String maybeReplaceVariables(Scanner scanner, Map<String, Object> variables) {
        String nextLine = scanner.nextLine().trim();
        if (isNotBlank(nextLine) && !variables.isEmpty()) {
            final Matcher matcher = VARIABLE_PATTERN.matcher(nextLine);
            while (matcher.find()) {
                final String group = matcher.group(1);
                Validator.validateTrue(variables.containsKey(group),
                        "Cannot find value for variable ${%s} in the variable map provided to ScriptExecutor", group);
                final String replacement = SPECIAL_REGEX_CHARS.matcher(variables.get(group).toString()).replaceAll("\\\\$0");
                nextLine = nextLine.replaceFirst("\\$\\{" + group + "\\}", replacement);

            }
        }
        return nextLine;
    }

protected List<SimpleStatement> buildStatements(List<String> lines) {
        List<SimpleStatement> statements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        boolean batch = false;
        boolean codeBlock = false;
        StringBuilder batchStatement = new StringBuilder();
        for (String line : lines) {
            if (line.trim().startsWith(BATCH_BEGIN)) {
                batch = true;
            }
            if (line.trim().matches(CODE_DELIMITER_START)) {
                if(codeBlock) {
                    codeBlock = false;
                } else {
                    codeBlock = true;
                }
            }

            if (batch) {
                batchStatement.append(" ").append(line);
                if (line.trim().startsWith(BATCH_APPLY)) {
                    batch = false;
                    statements.add(new SimpleStatement(batchStatement.toString()));
                    batchStatement = new StringBuilder();
                }
            } else if(codeBlock) {
                statement.append(line);
                if (line.trim().matches(CODE_DELIMITER_END)) {
                    codeBlock = false;
                    statements.add(new SimpleStatement(statement.toString()));
                    statement = new StringBuilder();
                }
            }
            else {
                statement.append(line);
                if (line.trim().endsWith(COMMA)) {
                    statements.add(new SimpleStatement(statement.toString()));
                    statement = new StringBuilder();
                } else {
                    statement.append(" ");
                }
            }
        }
        return statements;
    }

    public Session getSession() {
        return session;
    }
}
