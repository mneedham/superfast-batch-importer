package org.neo4j.kernel.api.Exceptions;

import org.neo4j.kernel.api.exceptions.KernelException;

public class BatchImportException extends KernelException
{

    public BatchImportException( String message,
                                 Object[] parameters )
    {
        super( message, parameters );
        // TODO Auto-generated constructor stub
    }

    public BatchImportException( String message )
    {
        super( message );
        // TODO Auto-generated constructor stub
    }
}
