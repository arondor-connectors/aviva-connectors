package com.arondor.viewer.oracle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.arondor.viewer.annotation.exceptions.AnnotationsNotSupportedException;
import com.arondor.viewer.client.api.document.DocumentId;
import com.arondor.viewer.client.api.document.metadata.DocumentMetadata;
import com.arondor.viewer.common.documentaccessor.DocumentAccessorByteArray;
import com.arondor.viewer.rendition.api.annotation.AnnotationAccessor;
import com.arondor.viewer.rendition.api.document.DocumentAccessor;

import oracle.sql.BFILE;

/**
 * Allows to access to document contained in a Oracle bfile field
 * 
 */
public class BFileOracleDocumentAccessor implements DocumentAccessor, Serializable
{
    private static final long serialVersionUID = 7713961986606863232L;

    private static final Logger LOGGER = Logger.getLogger(AvivaURLParser.class);

    private AnnotationAccessor annotationAccessor;

    private String title;

    private DocumentMetadata documentMetadata;

    private String query;

    private DataSource dataSource;

    private long oracleDocumentId;

    private DocumentId uuid;

    public BFileOracleDocumentAccessor(long oracleDocumentId, DocumentId uuid)
    {
        this.oracleDocumentId = oracleDocumentId;
        this.uuid = uuid;
    }

    public InputStream getInputStream() throws IOException
    {

        return new ByteArrayInputStream(toByteArray());
    }

    public byte[] toByteArray() throws IOException
    {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        BFILE bFile = null;
        try
        {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, oracleDocumentId);

            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.next())
            {
                LOGGER.error("No result can be found for oracleDocumentId=" + oracleDocumentId + " and documentId="
                        + getUUID());
                throw new IOException("No result can be found for oracleDocumentId=" + oracleDocumentId
                        + " and documentId=" + getUUID());
            }
            else
            {
                bFile = (BFILE) resultSet.getObject(1);
                checkBFileNotNull(bFile);
                bFile.openFile();
                byte[] stream = bFile.getBytes(1, (int) bFile.length());
                return stream;
            }
        }
        catch (SQLException e)
        {
            LOGGER.error("Cannot fetch document content: Impossible to update fetch using oracleDocumentId="
                    + oracleDocumentId + " and documentId=" + getUUID(), e);
            throw new IOException(e);
        }
        finally
        {
            try 
            {
            	if (bFile != null)
            	{
            		try
            		{
            			bFile.closeFile();
            		}
            		catch (SQLException e)
            		{
            			LOGGER.error("Could not close BFile corresponding to F_DocNumber : " + oracleDocumentId);
            		}
            		AbstractDAO.close(preparedStatement);
            	}
            }
            finally
            {            	
            	if (connection != null)
            		AbstractDAO.close(connection);
            }
            
            
        }
    }

    private void checkBFileNotNull(BFILE bFile)
    {
        if (bFile == null)
        {
            throw new IllegalArgumentException(
                    "The supplied documentId (" + getUUID() + ") corresponds to a null BFile");
        }
    }

    public DocumentAccessor asSerializableDocumentAccessor() throws IOException
    {
        return new DocumentAccessorByteArray(getUUID(), getInputStream());
    }

    public DocumentMetadata getDocumentMetadata()
    {
        return documentMetadata;
    }

    public String getDocumentTitle()
    {
        return title;
    }

    public void setDocumentTitle(String title)
    {
        this.title = title;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public DocumentId getUUID()
    {
        return uuid;
    }

    public AnnotationAccessor getAnnotationAccessor() throws AnnotationsNotSupportedException
    {
        return annotationAccessor;
    }

    public void setAnnotationAccessor(AnnotationAccessor annotationAccessor) throws AnnotationsNotSupportedException
    {
        this.annotationAccessor = annotationAccessor;
    }

    public String getMimeType() throws IOException
    {
        return null;
    }

    public String getPath() throws IOException
    {
        return null;
    }

    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }
}
