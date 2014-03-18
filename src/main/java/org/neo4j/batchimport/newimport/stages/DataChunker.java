package org.neo4j.batchimport.newimport.stages;

import java.io.IOException;
import java.io.BufferedReader;
import java.util.Arrays;

import org.neo4j.batchimport.newimport.structs.CSVDataBuffer;
import org.neo4j.batchimport.newimport.structs.Constants;


/**
* @author mh
* @since 13.11.12
*/
public class DataChunker {
    public static final String EOF = null;
    public static final String EOL = "\n".intern();
    public static final String NO_VALUE = "".intern();
    public static final char EOL_CHAR = '\n';
    public static final char EOF_CHAR = (char)-1;
    public static final int PREV_EOL_CHAR = -2;
    private static final int BUFSIZE = 32*1024;
    private final BufferedReader reader;
    private char delim = '\t';
    private final char[] buffer=new char[BUFSIZE];
    private int lastChar = PREV_EOL_CHAR;
    private int pos=BUFSIZE;
    int sequenceId = 0;
    private boolean seenEOF = false;
    
    StringBuilder curStrBuf = null;
    int cursorPos = 0;
    public long linesRead = 0;
    int recType = -1;

    public DataChunker(BufferedReader reader, char delim) {
        this.reader = reader;
        this.delim = delim;
    }
    public void setRecordType(int type){
    	recType = type;
    }

    synchronized int fillRawData(CSVDataBuffer buf)throws IOException{  	
    	int fillSize = Constants.BUFFER_SIZE_BYTES;//buf.getStrBuf().capacity() - 100;
    	char[] rawBuffer=new char[fillSize];
    	int available = 0;
    	buf.cleanup();
    	buf.setBufSequenceId(sequenceId++);
  
    	if (pos > 0){
    		available = BUFSIZE-pos;
    		System.arraycopy(buffer, pos, rawBuffer, 0, available);
    		pos = 0;
    	} else {
    		available = reader.read(rawBuffer, 0, fillSize-1);
    		if (available == -1 && seenEOF)
	    		return available;
    		if (available == fillSize-1){
    			lastChar = reader.read();
    			if (lastChar == -1){
    				rawBuffer[available++] = EOF_CHAR;
    				seenEOF = true;
    			}
    			else
    				rawBuffer[available++] = (char)lastChar;
    		}
	    	if (available < fillSize-1){
	    		//EOF
	    		rawBuffer[available++] = EOF_CHAR;
	    		seenEOF = true;
	    		buf.getStrBuf().append(rawBuffer, 0, available);
	    		return available;
	    	}    	
    	}
	    //read till the next EOL or EOF
	    buf.getStrBuf().append(rawBuffer, 0, available);
	    char[] ch = new char[1];
	    while (true){
	    	if (reader.read(ch, 0, 1) == -1){
	    		buf.getStrBuf().append(EOF_CHAR);
	    		seenEOF = true;
	    		break;
	    	}
	    	buf.getStrBuf().append(ch[0]);
	    	if (ch[0] == EOL_CHAR)
	    		break;
	    }
    	//return buf.strBuf.charAt(buf.strBuf.length()-1) != EOF_CHAR;
	    return available;
    }
    
    public boolean fillBuffer(CSVDataBuffer buf)throws IOException{
    	if (fillRawData(buf) < 0)
    		return false;
    	//parse
    	try{
    	int[][] record = new int[buf.getNumColumns()][2];
    	int colIndex = 0;
    	int wordStart =0, pos = 0;
    	int ch;
    	boolean EOF = false;
    	while (!EOF && pos < buf.getStrBuf().length()){
    		ch = buf.getStrBuf().charAt(pos);
    		if (ch == delim || ch == EOL_CHAR || ch == EOF_CHAR){
    			record[colIndex][0] = wordStart;
    			record[colIndex][1]	= pos;
    			wordStart = pos + 1;
    			colIndex++;
    			if (ch == EOL_CHAR || ch == EOF_CHAR){
    				if (ch == EOF_CHAR)
    					EOF = true;

    				//add record only if there is data
    				for (int i = 0; i < record.length; i++) 
    					if (record[i][0] != record[i][1]){ 
    						try {
    							buf.addRecord(record, recType);
    						}catch (Exception e){
    							System.out.println("Chunker1-"+e.getMessage());
    						}
    						break;
    					}

    				colIndex = 0;			
    				for (int i = 0; i < record.length; i++)
        				Arrays.fill(record[i], 0); 
    			}
    		}
    		pos++;
    	}
    	return !EOF;
    	}catch (Exception e){
    		System.out.println("Chunker2"+e.getMessage());
    		return false;
    	}
    }
}
