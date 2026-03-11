package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents inlined data rows stored directly in the catalog database.
 * Each row is a Map of column_name -> string_value.
 * Uses ArrayList and LinkedHashMap which are both Serializable.
 */
public class DuckLakeInlinedInputPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 1L;

    private final ArrayList<Map<String, String>> rows;

    public DuckLakeInlinedInputPartition(ArrayList<Map<String, String>> rows) {
        this.rows = rows;
    }

    public ArrayList<Map<String, String>> getRows() {
        return rows;
    }
}
