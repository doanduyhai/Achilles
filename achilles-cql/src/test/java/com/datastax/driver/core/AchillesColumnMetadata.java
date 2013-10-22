package com.datastax.driver.core;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class AchillesColumnMetadata extends ColumnMetadata {

    public AchillesColumnMetadata(TableMetadata table, String name, DataType type, Row row) {
        super(table, name, type, row);
    }
}
