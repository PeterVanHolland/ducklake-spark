package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single Parquet file to read from a DuckLake table.
 * Carries column metadata for schema evolution (field_id mapping, defaults).
 */
public class DuckLakeInputPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 1L;

    private final String filePath;
    private final long recordCount;
    private final String[] deleteFilePaths;
    private final Map<Long, String> nameMapping;
    private final Map<Long, String> colIdToName;

    // Schema evolution support
    private final Map<String, Long> nameToColumnId;    // current column name -> column_id (field_id)
    private final Map<Long, String> columnDefaults;    // column_id -> initialDefault value (SQL literal or null)
    private final Map<Long, String> columnTypes;       // column_id -> DuckLake type string

    public DuckLakeInputPartition(String filePath, long recordCount,
                                   String[] deleteFilePaths,
                                   Map<Long, String> nameMapping,
                                   Map<Long, String> colIdToName,
                                   Map<String, Long> nameToColumnId,
                                   Map<Long, String> columnDefaults,
                                   Map<Long, String> columnTypes) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.deleteFilePaths = deleteFilePaths;
        this.nameMapping = nameMapping;
        this.colIdToName = colIdToName;
        this.nameToColumnId = nameToColumnId;
        this.columnDefaults = columnDefaults;
        this.columnTypes = columnTypes;
    }

    public String getFilePath() { return filePath; }
    public long getRecordCount() { return recordCount; }
    public String[] getDeleteFilePaths() { return deleteFilePaths; }
    public Map<Long, String> getNameMapping() { return nameMapping; }
    public Map<Long, String> getColIdToName() { return colIdToName; }
    public Map<String, Long> getNameToColumnId() { return nameToColumnId; }
    public Map<Long, String> getColumnDefaults() { return columnDefaults; }
    public Map<Long, String> getColumnTypes() { return columnTypes; }

    /** True if this partition has delete files that require row-by-row filtering. */
    public boolean hasDeleteFiles() {
        return deleteFilePaths != null && deleteFilePaths.length > 0;
    }

    /** True if this partition has a name mapping (column renames requiring row-by-row mapping). */
    public boolean hasNameMapping() {
        return nameMapping != null && !nameMapping.isEmpty();
    }
}
