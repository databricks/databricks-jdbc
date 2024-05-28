package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import java.sql.*;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;

@ExtendWith(MockitoExtension.class)
public class DatabricksUCVolumeClientTest {

    private static final String CATALOG = "samikshya_hackathon";

    private static final String SCHEMA = "agnipratim_test";

    @Mock Connection connection;

    @Mock Statement statement;

    @Mock ResultSet resultSet;

    @Test
    public void testPrefixExists() throws SQLException {
        // Arrange
        // DatabricksUCVolumeSdkClient client = new DatabricksUCVolumeSdkClient(JDBC_URL, USER, PASSWORD, CON, STATEMENT);
        String volume = "abc_volume1";
        String prefix = "efg";

        DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

        // Define the behavior of the mock objects
        when(connection.createStatement()).thenReturn(statement);
        String listFilesSQL = "LIST '/Volumes/"  + CATALOG + "/" + SCHEMA + "/" + volume + "/'";
        when(statement.executeQuery(listFilesSQL)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, true, false); // Simulate four rows in the result set
        when(resultSet.getString("name")).thenReturn("abc_file1", "abc_file2", "def_file1", "efg_file2");

        // Act
        Statement mock_statement = connection.createStatement();
        boolean exists = client.prefixExists(CATALOG, SCHEMA, volume, prefix, mock_statement);

        // Assert
        assertTrue(exists);
        verify(statement).executeQuery("LIST '/Volumes/"  + CATALOG + "/" + SCHEMA + "/" + volume + "/'");
        //verify(resultSet, times(4)).next(); // Verify that next() is called four times
        //parametrised test

    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    public void testPrefixExists(String volume, String prefix, boolean expected) throws SQLException {
        // Arrange
        DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

        // Define the behavior of the mock objects
        when(connection.createStatement()).thenReturn(statement);
        String listFilesSQL = "LIST '/Volumes/"  + CATALOG + "/" + SCHEMA + "/" + volume + "/'";
        when(statement.executeQuery(listFilesSQL)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, true, false); // Simulate four rows in the result set
        when(resultSet.getString("name")).thenReturn("abc_file1", "abc_file2", "def_file1", "efg_file2");

        // Act
        Statement mock_statement = connection.createStatement();
        boolean exists = client.prefixExists(CATALOG, SCHEMA, volume, prefix, mock_statement);

        // Assert
        assertEquals(expected, exists);
        verify(statement).executeQuery("LIST '/Volumes/"  + CATALOG + "/" + SCHEMA + "/" + volume + "/'");
    }

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of("abc_volume1", "efg", true),
                Arguments.of("abc_volume2", "xyz", false)
                // Add more sets of parameters as needed
        );
    }
}