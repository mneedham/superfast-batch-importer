package org.neo4j.kernel.api.Exceptions;

import org.neo4j.kernel.api.exceptions.KernelException;

public class BatchImportException extends KernelException
{
    public BatchImportException( String message, Object... parameters )
    {
        super( message, parameters );
    }

    public BatchImportException( String message, Throwable e )
    {
        super( e, message );
    }
}
