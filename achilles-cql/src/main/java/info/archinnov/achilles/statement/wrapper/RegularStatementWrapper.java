package info.archinnov.achilles.statement.wrapper;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class RegularStatementWrapper extends AbstractStatementWrapper {

	private RegularStatement regularStatement;

	public RegularStatementWrapper(RegularStatement regularStatement, Object[] boundValues,
			ConsistencyLevel consistencyLevel) {
		super(boundValues);
		this.regularStatement = regularStatement;
		regularStatement.setConsistencyLevel(consistencyLevel);
	}

	@Override
	public ResultSet execute(Session session) {
		logDMLStatement(false, "");
		return session.execute(regularStatement);
	}

	@Override
	public Statement getStatement() {
		return regularStatement;
	}

	@Override
	public void logDMLStatement(boolean isBatch, String indentation) {
		if (dmlLogger.isDebugEnabled()) {
			String queryType = "Parameterized statement";
			String queryString = regularStatement.getQueryString();
			String consistencyLevel = regularStatement.getConsistencyLevel() == null ? "DEFAULT" : regularStatement
					.getConsistencyLevel().name();
			writeDMLStatementLog(isBatch, queryType, queryString, consistencyLevel, values);
		}
	}
}
