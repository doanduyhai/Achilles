package info.archinnov.achilles.statement.wrapper;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public abstract class AbstractStatementWrapper {
	public static final String ACHILLES_DML_STATEMENT = "ACHILLES_DML_STATEMENT";
	protected static final Logger dmlLogger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

	protected Object[] values = new Object[] {};

	protected AbstractStatementWrapper(Object[] values) {
		if (ArrayUtils.isNotEmpty(values))
			this.values = values;
	}

	public Object[] getValues() {
		return values;
	}

	public abstract ResultSet execute(Session session);

	public abstract Statement getStatement();

	public abstract void logDMLStatement(boolean isBatch, String indentation);

	public static void writeDMLStartBatch() {
		if (dmlLogger.isDebugEnabled()) {
			dmlLogger.debug("******BATCH START******");
		}
	}

	public static void writeDMLEndBatch(ConsistencyLevel consistencyLevel) {
		if (dmlLogger.isDebugEnabled()) {
			dmlLogger.debug("******BATCH END with CONSISTENCY LEVEL [{}] ******", consistencyLevel);
		}
	}

	protected void writeDMLStatementLog(boolean isBatch, String queryType, String queryString, String consistencyLevel,
			Object... values) {
		if (!isBatch)
			dmlLogger.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", queryType, queryString, consistencyLevel);
		else
			dmlLogger.debug("{} : [{}]", queryType, queryString);

		if (ArrayUtils.isNotEmpty(values)) {
			dmlLogger.debug("\t bound values : {}", Arrays.asList(values));
		}
	}
}
