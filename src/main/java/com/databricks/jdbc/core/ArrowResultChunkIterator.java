package com.databricks.jdbc.core;

/**
 * Iterator class to iterate through an ArrowResultChunk.
 * An ArrowResultChunk is a collection of Arrow RecordBatches.
 * Each RecordBatch contains multiple rows via a list of column vectors.
 * We iterate through the rows of a RecordBatch and then move to the next RecordBatch once we reach the end of the current one.
 */
public class ArrowResultChunkIterator {
    private final ArrowResultChunk resultChunk;

    private boolean begunIterationOverChunk;
    private int recordBatchesInChunk;

    private int recordBatchCursorInChunk;

    private int rowsInRecordBatch;

    private int rowCursorInRecordBatch;

    ArrowResultChunkIterator(ArrowResultChunk resultChunk) {
        this.resultChunk = resultChunk;
        this.begunIterationOverChunk = false;
        this.recordBatchesInChunk = resultChunk.getRecordBatchCountInChunk();
        this.recordBatchCursorInChunk = 0;
        this.rowsInRecordBatch = 0; // unimplemented, will be set to row size of first record batch
        this.rowCursorInRecordBatch = 0;
    }

    public void nextRow() {
        throw new UnsupportedOperationException("Not implemented");
    }
}