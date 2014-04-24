package org.neo4j.kernel.api.Exceptions;

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

public class BatchImportException extends KernelException
{
    public BatchImportException( String message, Object... parameters )
    {
        super( Status.General.UnknownFailure, message, parameters );
    }

    public BatchImportException( String message, Throwable e )
    {
        super( Status.General.UnknownFailure, e, message );
    }
}
