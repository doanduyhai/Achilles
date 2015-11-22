package info.archinnov.achilles.internals.parser.context;

import com.squareup.javapoet.CodeBlock;

import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;

public class FieldInfoContext {

    public final CodeBlock codeBlock;
    public final String fieldName;
    public final String cqlColumn;
    public final ColumnType columnType;
    public final ColumnInfo columnInfo;

    public FieldInfoContext(CodeBlock codeBlock, String fieldName, String cqlColumn, ColumnType columnType, ColumnInfo columnInfo) {
        this.codeBlock = codeBlock;
        this.fieldName = fieldName;
        this.cqlColumn = cqlColumn;
        this.columnType = columnType;
        this.columnInfo = columnInfo;
    }
}
