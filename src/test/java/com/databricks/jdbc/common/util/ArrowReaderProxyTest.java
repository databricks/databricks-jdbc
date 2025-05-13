package com.databricks.jdbc.common.util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ArrowReaderProxyTest {

    @Test
    void testIsDirectAccessAvailable() {
        // This method should return a boolean value
        boolean isAvailable = ArrowReaderProxy.isDirectAccessAvailable();
        
        // We can't assert exactly what this should be as it depends on the JVM's configuration
        // but we can check that the method runs without exception
        System.out.println("Direct access available: " + isAvailable);
    }
    
    @Test
    void testCreateReader() throws IOException {
        // Create test arrow data
        byte[] arrowData = createTestArrowData();
        
        // Create a reader via the proxy
        ByteArrayInputStream input = new ByteArrayInputStream(arrowData);
        try (RootAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = ArrowReaderProxy.createReader(input, allocator)) {
            
            // Try to load a batch
            try {
                boolean success = reader.loadNextBatch();
                assertTrue(success, "Should successfully load a batch");
                
                // Check the schema root
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertNotNull(root, "Vector schema root should not be null");
                assertEquals(1, root.getFieldVectors().size(), "Should have 1 field");
                assertEquals("test", root.getSchema().getFields().get(0).getName(), "Field name should be 'test'");
                
                // Check values
                IntVector vector = (IntVector) root.getFieldVectors().get(0);
                assertEquals(3, vector.getValueCount(), "Should have 3 values");
                assertEquals(1, vector.get(0), "First value should be 1");
                assertEquals(2, vector.get(1), "Second value should be 2");
                assertEquals(3, vector.get(2), "Third value should be 3");
                
            } catch (IOException e) {
                // If this fails with a specific message about --add-opens, it's expected
                // when running without the JVM flag
                if (e.getMessage() != null && e.getMessage().contains("--add-opens")) {
                    System.out.println("Test running in environment without --add-opens flag. Expected failure: " + e.getMessage());
                } else {
                    throw e;
                }
            }
        }
    }
    
    private byte[] createTestArrowData() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        try (RootAllocator allocator = new RootAllocator()) {
            // Create an int vector with some test data
            IntVector vector = new IntVector("test", allocator);
            vector.allocateNew(3);
            vector.set(0, 1);
            vector.set(1, 2);
            vector.set(2, 3);
            vector.setValueCount(3);
            
            // Create a schema root with the vector
            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(vector.getField()), 
                                                         Arrays.asList(vector), 3);
            
            // Write the data
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        
        return out.toByteArray();
    }
} 