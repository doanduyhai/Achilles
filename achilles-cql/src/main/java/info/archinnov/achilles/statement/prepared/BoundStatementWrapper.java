package info.archinnov.achilles.statement.prepared;

import com.datastax.driver.core.BoundStatement;

public class BoundStatementWrapper {

    private BoundStatement bs;

    private Object[] values;

    public BoundStatementWrapper(BoundStatement bs, Object[] values) {
        this.bs = bs;
        this.values = values;
    }

    public BoundStatement getBs() {
        return bs;
    }

    public Object[] getValues() {
        return values;
    }

}
