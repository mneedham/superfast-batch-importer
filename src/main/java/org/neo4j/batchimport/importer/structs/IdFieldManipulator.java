package org.neo4j.batchimport.importer.structs;

public class IdFieldManipulator
{
    // MSB visited, count, id LSB

    public static final int COUNT_BITS = 28;
    public static final int MAX_COUNT = (1 << COUNT_BITS) - 1;
    public static final int ID_BITS = 64-(COUNT_BITS+1);
    private static final long COUNT_BIT_MASK = 0x7FFFFFF8_00000000L;
    private static final long VISITED_BIT_MASK = 0x80000000_00000000L;
    private static final long ID_BIT_MASK = ~(COUNT_BIT_MASK | VISITED_BIT_MASK);

    public static long setId( long field, long id )
    {
        return (field & ~ID_BIT_MASK) | id;
    }

    public static long getId( long field )
    {
        long result = field & ID_BIT_MASK;
        return result == ID_BIT_MASK ? -1 : result;
    }

    public static long changeCount( long field, int diff )
    {
        return setCount( field, getCount( field )+diff );
    }

    public static int getCount( long field )
    {
        return (int) ((field & COUNT_BIT_MASK) >>> ID_BITS);
    }

    public static long setCount( long field, int count )
    {
        long otherBits = field & ~COUNT_BIT_MASK;
        if ( count < 0 || count > MAX_COUNT )
        {
            throw new IllegalStateException( "tried to decrement counter below zero." );
        }
        long longCount = count;
        return (longCount << ID_BITS) | otherBits;
    }

    public static boolean isVisited( long field )
    {
        return (field & VISITED_BIT_MASK) != 0;
    }

    public static long setVisited( long field )
    {
        return field | VISITED_BIT_MASK;
    }

    public static long cleanId( long field )
    {
        long otherBits = field & ~ID_BIT_MASK;
        return otherBits | ID_BIT_MASK;
    }

    public static long emptyField()
    {
        return ID_BIT_MASK;
    }
}
