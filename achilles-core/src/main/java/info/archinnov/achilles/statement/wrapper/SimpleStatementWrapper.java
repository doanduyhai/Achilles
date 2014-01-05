package info.archinnov.achilles.statement.wrapper;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class SimpleStatementWrapper extends AbstractStatementWrapper {

	private SimpleStatement simpleStatement;

	public SimpleStatementWrapper(String query, Object[] values) {
		super(values);
		this.simpleStatement = new SimpleStatement(query);
	}

	@Override
	public ResultSet execute(Session session) {
		logDMLStatement("");
		return session.execute(simpleStatement.getQueryString(), values);
	}

	@Override
	public SimpleStatement getStatement() {
		return simpleStatement;
	}

	@Override
	public void logDMLStatement(String indentation) {
		if (dmlLogger.isDebugEnabled()) {
			String queryType = "Simple statement";
			String queryString = simpleStatement.getQueryString();
			String consistencyLevel = simpleStatement.getConsistencyLevel() == null ? "DEFAULT" : simpleStatement
					.getConsistencyLevel().name();
			writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
		}
	}
}
