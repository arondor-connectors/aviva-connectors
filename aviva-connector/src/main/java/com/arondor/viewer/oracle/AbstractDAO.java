package com.arondor.viewer.oracle;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class AbstractDAO
{
    private static final Logger LOGGER = Logger.getLogger(AbstractDAO.class);

    public static void close(Connection connection)
    {
        try
        {
            if (connection != null)
            {
                connection.close();
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("The JDBC connection cannot be closed", e);
        }
    }

    public static void close(Statement statement)
    {
        try
        {
            if (statement != null)
            {
                statement.close();
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("The JDBC statement cannot be closed", e);
        }
    }
}