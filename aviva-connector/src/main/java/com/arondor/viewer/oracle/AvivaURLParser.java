package com.arondor.viewer.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.arondor.viewer.annotation.exceptions.AnnotationsNotSupportedException;
import com.arondor.viewer.client.api.document.DocumentContainer;
import com.arondor.viewer.client.api.document.DocumentFormatNotSupportedException;
import com.arondor.viewer.client.api.document.DocumentId;
import com.arondor.viewer.client.api.document.DocumentLayout;
import com.arondor.viewer.client.api.document.DocumentNotAvailableException;
import com.arondor.viewer.client.api.document.DocumentReference;
import com.arondor.viewer.client.api.document.id.DocumentIdParameter;
import com.arondor.viewer.common.document.DocumentLayoutDocument;
import com.arondor.viewer.common.document.id.DocumentIdFactory;
import com.arondor.viewer.common.document.id.URLDocumentIdParameter;
import com.arondor.viewer.common.util.DependencyInjection;
import com.arondor.viewer.rendition.api.DocumentServiceURLParser;
import com.arondor.viewer.rendition.api.annotation.AnnotationAccessor;
import com.arondor.viewer.rendition.api.document.DocumentService;

public class AvivaURLParser implements DocumentServiceURLParser
{
    private static final String ORACLE_ID_KEY = "documentId";

    private static final Logger LOGGER = Logger.getLogger(AvivaURLParser.class);

    private DataSource dataSource;

    private String query;

    @Override
    public boolean canParse(DocumentService arg0, ServletContext arg1, HttpServletRequest request)
    {
        return request.getParameter(ORACLE_ID_KEY) != null;
    }

    @Override
    public DocumentId parse(DocumentService documentService, ServletContext context, HttpServletRequest request)
            throws DocumentNotAvailableException, DocumentFormatNotSupportedException
    {
        LOGGER.info("Parsing document for queryString=" + request.getQueryString());

        String[] ids = request.getParameterValues(ORACLE_ID_KEY);

        if (ids.length == 1)
        {
            String oracleDocumentId = ids[0];
            LOGGER.info("Load mono document for oracle id: " + oracleDocumentId);
            BFileOracleDocumentAccessor da = loadMonoDocument(documentService, oracleDocumentId);
            return da.getUUID();
        }
        else if (ids.length > 1)
        {
            LOGGER.info("Load multi document for oracle ids: " + Arrays.toString(ids));
            DocumentContainer container = new DocumentContainer();
            container.setChildren(new ArrayList<DocumentLayout>());

            List<DocumentIdParameter> parameters = new ArrayList<DocumentIdParameter>();

            for (String oracleDocumentId : ids)
            {
                BFileOracleDocumentAccessor da = loadMonoDocument(documentService, oracleDocumentId);

                DocumentReference ref = new DocumentReference();
                ref.setDocumentId(da.getUUID());
                ref.setDocumentTitle(da.getDocumentTitle());
                container.getChildren().add(ref);
                parameters.add(new URLDocumentIdParameter(ORACLE_ID_KEY, oracleDocumentId));
            }
            container.setDocumentId(DocumentIdFactory.getInstance().generate(parameters));

            DocumentLayoutDocument multiDocument = new DocumentLayoutDocument(container);
            documentService.loadDocument(multiDocument);
            LOGGER.info("Multi document " + multiDocument.getDocumentId() + " has been loaded");
            return multiDocument.getDocumentId();
        }

        return null;
    }

    private BFileOracleDocumentAccessor loadMonoDocument(DocumentService documentService, String oracleDocumentId)
            throws DocumentNotAvailableException, DocumentFormatNotSupportedException
    {
        List<DocumentIdParameter> parameters = new ArrayList<DocumentIdParameter>();
        parameters.add(new URLDocumentIdParameter(ORACLE_ID_KEY, oracleDocumentId));
        DocumentId uuid = DocumentIdFactory.getInstance().generate(parameters);
        BFileOracleDocumentAccessor da = new BFileOracleDocumentAccessor(Long.parseLong(oracleDocumentId), uuid);
        da.setDataSource(dataSource);
        da.setQuery(query);

        try
        {
            Object[] params = { documentService, da };
            AnnotationAccessor annotationAccessor = (AnnotationAccessor) DependencyInjection
            		.getBean("avivaAnnotationAccessor", params);
            da.setAnnotationAccessor(annotationAccessor);
        }
        catch (AnnotationsNotSupportedException e)
        {
            LOGGER.error("Annotation accessor cannot be set on " + da + " for documentId=" + da.getUUID(), e);
        }

        documentService.loadDocumentAccessor(da);
        return da;
    }

    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

}
