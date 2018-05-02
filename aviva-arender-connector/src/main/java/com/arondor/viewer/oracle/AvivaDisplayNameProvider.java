package com.arondor.viewer.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.arondor.viewer.common.user.DisplayNameProvider;

public class AvivaDisplayNameProvider implements DisplayNameProvider {
	
	private static final Logger LOGGER = Logger.getLogger(AvivaDisplayNameProvider.class);

    private String query;

	private DataSource dataSource;

    public String fetchDisplayName(String originalCreatorName)
    {
        return getUserName(originalCreatorName);
    }

    public List<String> fetchDisplayNames(List<String> originalCreatorNames)
    {
        List<String> prefixed = new ArrayList<String>();
        for (String originalCreatorName : originalCreatorNames)
        {
            prefixed.add(getUserName(originalCreatorName));
        }
        return prefixed;
    }

    private String getUserName(String originalCreatorName)
    {
        LOGGER.info("Determining first name and last name of " + originalCreatorName + ".");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try
        {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, originalCreatorName);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.next())
            {
                LOGGER.info("Cannot determine first name and last name of " + originalCreatorName + ". Setting " + originalCreatorName + " as name.");
                return originalCreatorName;
            }
            else
            {
                String firstName = resultSet.getString(1);
                String lastName = resultSet.getString(2);
                return firstName + " " + lastName;
            }
        }
        catch (SQLException e)
        {
            LOGGER.info("Cannot determine first name and last name of " + originalCreatorName + ". Setting " + originalCreatorName + " as name.");
            return originalCreatorName;
        }
        finally
        {
            AbstractDAO.close(preparedStatement);
            AbstractDAO.close(connection);
        }
    }
    
    public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
