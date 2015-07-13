package info.archinnov.achilles.internal.async;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

import java.util.List;

public class RowsWithExecutionInfo {

    private final List<Row> rows;
    private final ExecutionInfo executionInfo;

    public RowsWithExecutionInfo(List<Row> rows, ExecutionInfo executionInfo) {
        this.rows = rows;
        this.executionInfo = executionInfo;
    }

    public List<Row> getRows() {
        return rows;
    }

    public ExecutionInfo getExecutionInfo() {
        return executionInfo;
    }
}
