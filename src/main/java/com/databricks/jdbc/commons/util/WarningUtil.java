package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.sql.SQLWarning;

public class WarningUtil {
    public static void addWarning(SQLWarning warning, String warningText) throws DatabricksSQLException {
        SQLWarning newWarning = new SQLWarning(warningText);
        if(warning == null){
            warning = newWarning;
        }
        else {
            warning.setNextWarning(newWarning);
        }
    }
}
