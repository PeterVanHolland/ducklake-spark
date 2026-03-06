package io.ducklake.spark;

import io.ducklake.spark.util.DuckLakeTypeMapping;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for DuckDB → Spark type mapping.
 */
public class DuckLakeTypesMappingTest {

    @Test
    public void testIntegerTypes() {
        assertEquals(DataTypes.ByteType, DuckLakeTypeMapping.toSparkType("TINYINT"));
        assertEquals(DataTypes.ShortType, DuckLakeTypeMapping.toSparkType("SMALLINT"));
        assertEquals(DataTypes.IntegerType, DuckLakeTypeMapping.toSparkType("INTEGER"));
        assertEquals(DataTypes.IntegerType, DuckLakeTypeMapping.toSparkType("INT"));
        assertEquals(DataTypes.LongType, DuckLakeTypeMapping.toSparkType("BIGINT"));
    }

    @Test
    public void testUnsignedTypes() {
        assertEquals(DataTypes.ShortType, DuckLakeTypeMapping.toSparkType("UTINYINT"));
        assertEquals(DataTypes.IntegerType, DuckLakeTypeMapping.toSparkType("USMALLINT"));
        assertEquals(DataTypes.LongType, DuckLakeTypeMapping.toSparkType("UINTEGER"));
        assertTrue(DuckLakeTypeMapping.toSparkType("UBIGINT") instanceof DecimalType);
    }

    @Test
    public void testFloatTypes() {
        assertEquals(DataTypes.FloatType, DuckLakeTypeMapping.toSparkType("FLOAT"));
        assertEquals(DataTypes.DoubleType, DuckLakeTypeMapping.toSparkType("DOUBLE"));
    }

    @Test
    public void testDecimalType() {
        DataType dt = DuckLakeTypeMapping.toSparkType("DECIMAL(10,2)");
        assertTrue(dt instanceof DecimalType);
        assertEquals(10, ((DecimalType) dt).precision());
        assertEquals(2, ((DecimalType) dt).scale());
    }

    @Test
    public void testStringTypes() {
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType("VARCHAR"));
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType("TEXT"));
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType("VARCHAR(255)"));
    }

    @Test
    public void testBooleanType() {
        assertEquals(DataTypes.BooleanType, DuckLakeTypeMapping.toSparkType("BOOLEAN"));
        assertEquals(DataTypes.BooleanType, DuckLakeTypeMapping.toSparkType("BOOL"));
    }

    @Test
    public void testTemporalTypes() {
        assertEquals(DataTypes.DateType, DuckLakeTypeMapping.toSparkType("DATE"));
        assertEquals(DataTypes.TimestampType, DuckLakeTypeMapping.toSparkType("TIMESTAMP"));
        assertEquals(DataTypes.TimestampType, DuckLakeTypeMapping.toSparkType("TIMESTAMP WITH TIME ZONE"));
    }

    @Test
    public void testBinaryType() {
        assertEquals(DataTypes.BinaryType, DuckLakeTypeMapping.toSparkType("BLOB"));
        assertEquals(DataTypes.BinaryType, DuckLakeTypeMapping.toSparkType("BYTEA"));
    }

    @Test
    public void testListType() {
        DataType dt = DuckLakeTypeMapping.toSparkType("LIST(INTEGER)");
        assertTrue(dt instanceof ArrayType);
        assertEquals(DataTypes.IntegerType, ((ArrayType) dt).elementType());
    }

    @Test
    public void testMapType() {
        DataType dt = DuckLakeTypeMapping.toSparkType("MAP(VARCHAR, INTEGER)");
        assertTrue(dt instanceof MapType);
        assertEquals(DataTypes.StringType, ((MapType) dt).keyType());
        assertEquals(DataTypes.IntegerType, ((MapType) dt).valueType());
    }

    @Test
    public void testStructType() {
        DataType dt = DuckLakeTypeMapping.toSparkType("STRUCT(name VARCHAR, age INTEGER)");
        assertTrue(dt instanceof StructType);
        StructType st = (StructType) dt;
        assertEquals(2, st.fields().length);
        assertEquals("name", st.fields()[0].name());
        assertEquals(DataTypes.StringType, st.fields()[0].dataType());
        assertEquals("age", st.fields()[1].name());
        assertEquals(DataTypes.IntegerType, st.fields()[1].dataType());
    }

    @Test
    public void testSpecialTypes() {
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType("UUID"));
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType("JSON"));
        assertTrue(DuckLakeTypeMapping.toSparkType("HUGEINT") instanceof DecimalType);
    }

    @Test
    public void testNullInput() {
        assertEquals(DataTypes.StringType, DuckLakeTypeMapping.toSparkType(null));
    }
}
