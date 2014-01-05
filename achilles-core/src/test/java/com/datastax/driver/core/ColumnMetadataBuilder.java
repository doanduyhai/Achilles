package com.datastax.driver.core;


public class ColumnMetadataBuilder {

    public static ColumnMetadata create(TableMetadata tableMeta, String name, DataType type) {
        ColumnMetadata.Raw raw = new ColumnMetadata.Raw(name, null, 0, type, false);
        return ColumnMetadata.fromRaw(tableMeta,raw);
    }
}
