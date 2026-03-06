package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single Parquet file to read from a DuckLake table.
 */
public class DuckLakeInputPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 1L;

    private final String filePath;
    private final long recordCount;
    private final String[] deleteFilePaths;
    private final Map<Long, String> nameMapping;
    private final Map<Long, String> colIdToName;

    public DuckLakeInputPartition(String filePath, long recordCount,
                                   String[] deleteFilePaths,
                                   Map<Long, String> nameMapping,
                                   Map<Long, String> colIdToName) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.deleteFilePaths = deleteFilePaths;
        this.nameMapping = nameMapping;
        this.colIdToName = colIdToName;
    }

    public String getFilePath() { return filePath; }
    public long getRecordCount() { return recordCount; }
    public String[] getDeleteFilePaths() { return deleteFilePaths; }
    public Map<Long, String> getNameMapping() { return nameMapping; }
    public Map<Long, String> getColIdToName() { return colIdToName; }
}
