package com.arondor.viewer.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.arondor.viewer.annotation.api.Annotation;
import com.arondor.viewer.annotation.common.AnnotationFlags;
import com.arondor.viewer.annotation.exceptions.AnnotationCredentialsException;
import com.arondor.viewer.annotation.exceptions.AnnotationNotAvailableException;
import com.arondor.viewer.annotation.exceptions.AnnotationsNotSupportedException;
import com.arondor.viewer.annotation.exceptions.InvalidAnnotationFormatException;
import com.arondor.viewer.client.api.annotation.AnnotationCreationPolicy;
import com.arondor.viewer.client.api.context.UserContext;
import com.arondor.viewer.rendition.api.document.DocumentAccessor;
import com.arondor.viewer.rendition.api.document.DocumentService;
import com.arondor.viewer.xfdf.annotation.XFDFAnnotationAccessor;

public class AvivaAnnotationAccessor extends XFDFAnnotationAccessor
{
	private static final Logger LOGGER = Logger.getLogger(AvivaAnnotationAccessor.class);

    private DataSource dataSource;

    private String query;

    private AnnotationCreationPolicy creationPolicy;

    public AvivaAnnotationAccessor(DocumentService documentService, DocumentAccessor documentAccessor)
    {
        super(documentService, documentAccessor);
        creationPolicy = new AnnotationCreationPolicy();
        creationPolicy.setTextAnnotationsSupportReply(false);
    }

    public void create(List<Annotation> annotations) throws AnnotationsNotSupportedException,
            InvalidAnnotationFormatException, AnnotationCredentialsException, AnnotationNotAvailableException
    {
        LOGGER.info("Creatings annotations: " + annotations);
        super.create(annotations);
        LOGGER.info(annotations.size() + " have been created");
    }

    public synchronized List<Annotation> get() throws AnnotationsNotSupportedException, InvalidAnnotationFormatException
    {
    	LOGGER.info("Getting annotations ...");
        String user = getUsername();
    	LOGGER.info("Getting annotations with user=" + user);
        List<Annotation> annotations = super.get();
        boolean isAdmin = isAdmin(user);
        LOGGER.info("Current user " + user + " isAdmin=" + isAdmin);
        for (Annotation annotation : annotations)
        {
            if (!isOwner(annotation, user) && !isAdmin)
            {
                LOGGER.info("Disabling any modification on annotation " + annotation.getId() + ", creator="
                        + annotation.getCreator() + " and current user=" + user);
                annotation.setFlags(annotation.getFlags() == null ? new AnnotationFlags() : annotation.getFlags());
                annotation.getFlags().setReadonly(true);
                annotation.getFlags().setLocked(true);
            }
            else
            {
                LOGGER.info("User is either annotation owner or admin");
            }
        }
        LOGGER.info("Returning annotations: " + annotations);
        return annotations;
    }

    private boolean isOwner(Annotation annotation, String user)
    {
        return user.equals(annotation.getCreator());
    }

    private boolean isAdmin(String user)
    {
        LOGGER.info("Determining if user '" + user + "' is admin or not");

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try
        {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, user);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.next())
            {
                LOGGER.error("Cannot determine if the user " + user + " is admin or not");
            }
            else
            {
                String isAdmin = resultSet.getString(1);
                return "O".equals(isAdmin);
            }
        }
        catch (SQLException e)
        {
            LOGGER.error("Cannot determine if the user " + user + " is admin or not", e);
        }
        finally
        {
            AbstractDAO.close(preparedStatement);
            AbstractDAO.close(connection);
        }
        return false;
    }

    protected String getUsername()
    {
        SecurityContext context = SecurityContextHolder.getContext();
        if (context != null)
        {
            Authentication authentication = context.getAuthentication();
            if (authentication != null && authentication.getPrincipal() != null)
            {
                UserContext userContext = (UserContext) authentication.getPrincipal();
                return userContext.getUsername();
            }
        }
        return "";
    }

    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public AnnotationCreationPolicy getCreationPolicy()
    {
        return creationPolicy;
    }

    public void setCreationPolicy(AnnotationCreationPolicy creationPolicy)
    {
        this.creationPolicy = creationPolicy;
    }
}
