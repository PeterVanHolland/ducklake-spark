package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.FileColumnStats;
import io.ducklake.spark.catalog.*;
import io.ducklake.spark.reader.DuckLakeFilterEvaluator;

import org.apache.spark.sql.sources.*;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for stats-based file pruning and the exception hierarchy.
 */
public class DuckLakeStatsFilteringTest {

    // ---------------------------------------------------------------
    // Filter evaluation tests
    // ---------------------------------------------------------------

    private DuckLakeFilterEvaluator makeEvaluator(String colName,
                                                    String min, String max,
                                                    long nullCount, long valueCount) {
        Map<String, FileColumnStats> stats = new HashMap<>();
        stats.put(colName, new FileColumnStats(0, min, max, nullCount, valueCount));
        return new DuckLakeFilterEvaluator(stats, valueCount + nullCount);
    }

    @Test
    public void testEqualToWithinRange() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertTrue(eval.mightMatch(new EqualTo("id", 50)));
    }

    @Test
    public void testEqualToOutOfRange() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertFalse(eval.mightMatch(new EqualTo("id", 200)));
    }

    @Test
    public void testEqualToBelowRange() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "100", 0, 100);
        assertFalse(eval.mightMatch(new EqualTo("id", 5)));
    }

    @Test
    public void testEqualToAtBoundary() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertTrue(eval.mightMatch(new EqualTo("id", 1)));
        assertTrue(eval.mightMatch(new EqualTo("id", 100)));
    }

    @Test
    public void testGreaterThanAboveMax() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertFalse(eval.mightMatch(new GreaterThan("id", 100)));
    }

    @Test
    public void testGreaterThanBelowMax() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertTrue(eval.mightMatch(new GreaterThan("id", 50)));
    }

    @Test
    public void testLessThanBelowMin() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "100", 0, 100);
        assertFalse(eval.mightMatch(new LessThan("id", 10)));
    }

    @Test
    public void testLessThanAboveMin() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "100", 0, 100);
        assertTrue(eval.mightMatch(new LessThan("id", 50)));
    }

    @Test
    public void testGreaterThanOrEqualAtMax() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertTrue(eval.mightMatch(new GreaterThanOrEqual("id", 100)));
    }

    @Test
    public void testLessThanOrEqualAtMin() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "100", 0, 100);
        assertTrue(eval.mightMatch(new LessThanOrEqual("id", 10)));
    }

    @Test
    public void testIsNullWithNulls() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "a", "z", 5, 95);
        assertTrue(eval.mightMatch(new IsNull("name")));
    }

    @Test
    public void testIsNullNoNulls() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "a", "z", 0, 100);
        assertFalse(eval.mightMatch(new IsNull("name")));
    }

    @Test
    public void testIsNotNullAllNulls() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", null, null, 100, 0);
        assertFalse(eval.mightMatch(new IsNotNull("name")));
    }

    @Test
    public void testIsNotNullSomeValues() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "a", "z", 5, 95);
        assertTrue(eval.mightMatch(new IsNotNull("name")));
    }

    @Test
    public void testInPartialMatch() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "50", 0, 100);
        assertTrue(eval.mightMatch(new In("id", new Object[]{5, 20, 100})));
    }

    @Test
    public void testInNoMatch() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "50", 0, 100);
        assertFalse(eval.mightMatch(new In("id", new Object[]{5, 7, 100})));
    }

    @Test
    public void testAndBothMatch() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertTrue(eval.mightMatch(new And(
                new GreaterThan("id", 10),
                new LessThan("id", 90)
        )));
    }

    @Test
    public void testAndOneSkips() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        assertFalse(eval.mightMatch(new And(
                new GreaterThan("id", 200),
                new LessThan("id", 90)
        )));
    }

    @Test
    public void testOrBothSkip() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "50", 0, 100);
        assertFalse(eval.mightMatch(new Or(
                new LessThan("id", 10),
                new GreaterThan("id", 50)
        )));
    }

    @Test
    public void testOrOneMatches() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "10", "50", 0, 100);
        assertTrue(eval.mightMatch(new Or(
                new EqualTo("id", 20),
                new GreaterThan("id", 100)
        )));
    }

    @Test
    public void testNotIsNull() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "a", "z", 5, 95);
        // NOT(IsNull) → IsNotNull → has values → true
        assertTrue(eval.mightMatch(new Not(new IsNull("name"))));
    }

    @Test
    public void testStringStartsWith() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "alice", "charlie", 0, 100);
        assertTrue(eval.mightMatch(new StringStartsWith("name", "bob")));
    }

    @Test
    public void testStringStartsWithNoMatch() {
        DuckLakeFilterEvaluator eval = makeEvaluator("name", "alice", "charlie", 0, 100);
        assertFalse(eval.mightMatch(new StringStartsWith("name", "zorro")));
    }

    @Test
    public void testUnknownColumn() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 100);
        // Unknown column → conservative true
        assertTrue(eval.mightMatch(new EqualTo("unknown_col", 42)));
    }

    @Test
    public void testEmptyFile() {
        DuckLakeFilterEvaluator eval = makeEvaluator("id", "1", "100", 0, 0);
        assertFalse(eval.mightMatch(new EqualTo("id", 50)));
    }

    // ---------------------------------------------------------------
    // compareValues tests
    // ---------------------------------------------------------------

    @Test
    public void testCompareIntegers() {
        assertEquals(0, DuckLakeFilterEvaluator.compareValues(42, "42"));
        assertTrue(DuckLakeFilterEvaluator.compareValues(50, "42") > 0);
        assertTrue(DuckLakeFilterEvaluator.compareValues(30, "42") < 0);
    }

    @Test
    public void testCompareLongs() {
        assertEquals(0, DuckLakeFilterEvaluator.compareValues(1000000000L, "1000000000"));
        assertTrue(DuckLakeFilterEvaluator.compareValues(2000000000L, "1000000000") > 0);
    }

    @Test
    public void testCompareDoubles() {
        assertEquals(0, DuckLakeFilterEvaluator.compareValues(3.14, "3.14"));
        assertTrue(DuckLakeFilterEvaluator.compareValues(3.15, "3.14") > 0);
    }

    @Test
    public void testCompareStrings() {
        assertEquals(0, DuckLakeFilterEvaluator.compareValues("hello", "hello"));
        assertTrue(DuckLakeFilterEvaluator.compareValues("world", "hello") > 0);
        assertTrue(DuckLakeFilterEvaluator.compareValues("abc", "hello") < 0);
    }

    @Test
    public void testCompareNullSafe() {
        assertEquals(0, DuckLakeFilterEvaluator.compareValues(null, "42"));
        assertEquals(0, DuckLakeFilterEvaluator.compareValues(42, null));
    }

    // ---------------------------------------------------------------
    // Exception hierarchy tests
    // ---------------------------------------------------------------

    @Test
    public void testDuckLakeException() {
        DuckLakeException ex = new DuckLakeException("test error");
        assertEquals("test error", ex.getMessage());
    }

    @Test
    public void testTableNotFoundException() {
        DuckLakeTableNotFoundException ex = new DuckLakeTableNotFoundException("main", "users");
        assertEquals("Table 'main.users' not found", ex.getMessage());
        assertEquals("main", ex.getSchemaName());
        assertEquals("users", ex.getTableName());
        assertTrue(ex instanceof DuckLakeException);
    }

    @Test
    public void testSchemaNotFoundException() {
        DuckLakeSchemaNotFoundException ex = new DuckLakeSchemaNotFoundException("analytics");
        assertEquals("Schema 'analytics' not found", ex.getMessage());
        assertTrue(ex instanceof DuckLakeException);
    }

    @Test
    public void testSnapshotNotFoundForVersion() {
        DuckLakeSnapshotNotFoundException ex = DuckLakeSnapshotNotFoundException.forVersion(42);
        assertEquals("Snapshot version 42 not found", ex.getMessage());
        assertTrue(ex instanceof DuckLakeException);
    }

    @Test
    public void testSnapshotNotFoundForTimestamp() {
        DuckLakeSnapshotNotFoundException ex = DuckLakeSnapshotNotFoundException.forTimestamp("2025-01-01");
        assertTrue(ex.getMessage().contains("2025-01-01"));
        assertTrue(ex instanceof DuckLakeException);
    }

    @Test
    public void testConcurrentModificationException() {
        DuckLakeConcurrentModificationException ex =
                new DuckLakeConcurrentModificationException(5, 7);
        assertEquals(5, ex.getExpectedSnapshot());
        assertEquals(7, ex.getActualSnapshot());
        assertTrue(ex instanceof DuckLakeException);
    }

    @Test
    public void testSchemaMismatchException() {
        Set<String> fileCols = new TreeSet<>(Arrays.asList("a", "b"));
        Set<String> tableCols = new TreeSet<>(Arrays.asList("x", "y"));
        DuckLakeSchemaMismatchException ex = new DuckLakeSchemaMismatchException(fileCols, tableCols);
        assertTrue(ex.getMessage().contains("[a, b]"));
        assertTrue(ex.getMessage().contains("[x, y]"));
        assertTrue(ex instanceof DuckLakeException);
    }
}
