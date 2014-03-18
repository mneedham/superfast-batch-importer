package org.neo4j.batchimport.newimport.stages;

import org.neo4j.batchimport.importer.AbstractLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.importer.Type;
import org.neo4j.batchimport.newimport.structs.CSVDataBuffer;
import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.batchimport.newimport.structs.RunData;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.BufferedReader;
import java.util.*;

import static org.neo4j.helpers.collection.MapUtil.map;

public class ReadFileData extends AbstractLineData {
	public static ThreadLocal<RunData> threadLocalPerf = new ThreadLocal<RunData>();
    private DataChunker dataChunker = null;
    protected BufferedReader reader;
    private char delim;
    private boolean useQuotes;
    CSVReader csvReader = null;
    int recType;
    int sequenceId = 0;

    public ReadFileData(BufferedReader reader, char delim, int offset, boolean useQuotes) {
        super(offset);
        this.reader = reader;
        this.delim = delim;
        this.useQuotes = useQuotes;
        if (useQuotes)
        	this.csvReader = new CSVReader(reader, delim,'"','\\',0,false,false);
        else
        	dataChunker = new DataChunker(reader, delim);
        String[] headRow = readRawRow();
        String last = headRow[headRow.length-1];
        if (last.endsWith("\r"))
        	headRow[headRow.length-1] =  last.substring(0, last.length()-1);
        initHeaders(createHeaders(headRow));
        createMapData(lineSize, offset);
    }
    public DataChunker getDataChunker(){
    	return this.dataChunker;
    }
    public void setRecordType(int type){
    	recType = type;
    	if (!useQuotes)
    		dataChunker.setRecordType(type);
    }
    
    public int getHeaderLength(){
    	return this.headers.length;
    }
    public int getOffset(){
    	return offset;
    }
    public int getExplicitLabelId(){
    	return this.explicitLabelId;
    }
    protected boolean readLine() {
    	return false;
    }
    
    public boolean fillBuffer(CSVDataBuffer buf)throws IOException{
    	if (!useQuotes)
    		return dataChunker.fillBuffer(buf);
    	if (csvReader == null)
    		return false;
    	return csvFillBuffer(buf);
    }
    String[] prevLine = null;
    private int getlength(String[] strArray){
    	int length = 0;
    	for (String str : strArray)
    		length += str.length();
    	return length;
    }
    
    protected synchronized boolean csvFillBuffer(CSVDataBuffer buf)throws IOException{
    	int[][] record = new int[buf.getNumColumns()][2];
    	String[] newLine = null;
    	int pos = 0;
    	buf.cleanup();
    	buf.setBufSequenceId(sequenceId++);
    	while (true){
    		newLine = csvReader.readNext();
    		if (newLine == null)
    			return false;
    		int column = 0;
    		for (String str: newLine){
    			record[column][0] = pos;
    			pos += str.length();
    			record[column][1] = pos;
    			column++;
    			buf.getStrBuf().append(str);
    		}
    		buf.addRecord(record, recType);
    		if (pos > Constants.BUFFER_SIZE_BYTES-200)
    			break;
    	}
    	return true;
    }
    protected String[] readRawRow() {   	
    	try {
    		if (useQuotes)
    			return csvReader.readNext();
    		String row = reader.readLine();
    		return row.split(String.valueOf(delim));
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	}
    }

    public String[] getTypeLabels(CSVDataBuffer buf, int index) {
    	try{
    		if (explicitLabelId == -1) 
    			return null;
    		Object labels = Type.LABEL.convert(buf.getString(index, explicitLabelId));
    		return labels instanceof String ? new String[]{ labels.toString() } : (String[]) labels;
    	} catch (Exception e){
    		System.out.println("getTypeLables"+e.getMessage());
    		return null;
    	}
    }

    protected Object convert(int column, String value) {
        try {
            return headers[column].type == Type.STRING ? value : headers[column].type.convert(value);
        } catch(Exception e) {
            // todo potentially skip?
            throw new RuntimeException("Error converting value row "+rows+" column "+headers[column]+" value "+value+" error: "+e.getClass().getSimpleName()+": "+e.getMessage(),e);
        }
    }
    public Map<String, Object> getProperties(CSVDataBuffer buf, int index) {
    	try {
    	int propertyCount=0;
        int notnull = 0;
        Object[] properties = new Object[buf.getRecords()[index].length*2];
        for (int i = 0; i < buf.getRecords()[index].length; i++) {
            if (buf.getRecords()[index][i][0] == buf.getRecords()[index][i][1]) continue;
            notnull++;
            if (i<offset || i == explicitLabelId) continue;
            if (!headers[i].type.isProperty()) continue;
            properties[propertyCount++]= headers[i].name;          
            properties[propertyCount++]= convert(i, buf.getString(index, i));
        }
        Object[] newData=new Object[propertyCount];
        System.arraycopy(properties,0,newData,0, propertyCount);
        return map(newData);
    	} catch (Exception e){
    		System.out.println("getProperties"+e.getMessage());
    		return null;
    	}
    }
    public Map<String, Map<String, Object>> getIndexData(Object[] data) {
        if (!hasIndex) return Collections.EMPTY_MAP;
        Map<String, Map<String, Object>> indexData = new HashMap<String, Map<String, Object>>();
        for (int column = offset; column < headers.length; column++) {
            Header header = headers[column];
            if (header.indexName == null) continue;
            Object val = data[column];
            if (val == null) continue;

            if (!indexData.containsKey(header.indexName)) {
                indexData.put(header.indexName, new HashMap<String, Object>());
            }
            indexData.get(header.indexName).put(header.name,val);
        }
        return indexData;
    }
  
    public Map<String, Map<String, Object>> getIndexData(CSVDataBuffer buf, int index) {
    	try {
        if (!hasIndex) return Collections.EMPTY_MAP;
        Map<String, Map<String, Object>> indexData = new HashMap<String, Map<String, Object>>();
        for (int column = offset; column < headers.length; column++) {
            Header header = headers[column];
            if (header.indexName == null) continue;
            
            Object val = null;
            if (buf.getRecords()[index][column][0] != buf.getRecords()[index][column][1])
            	val = buf.getString(index, column);//data[column];
            if (val == null) continue;

            if (!indexData.containsKey(header.indexName)) {
                indexData.put(header.indexName, new HashMap<String, Object>());
            }
            indexData.get(header.indexName).put(header.name,val);
        }
        return indexData;
    	} catch (Exception e){
    		System.out.println("getIndexData"+e.getMessage());
    		return null;
    	}
    }
    public Map<String, Object> getIndexData(CSVDataBuffer buf, int index, int column) {
    	try {
        if (!hasIndex) return null;
        Map<String, Object> indexData = new HashMap<String, Object>();
        //for (int column = offset; column < headers.length; column++) {
            Header header = headers[column];
            if (header.indexName == null) return null;
            
            Object val = null;
            if (buf.getRecords()[index][column][0] != buf.getRecords()[index][column][1])
            	val = buf.getString(index, column);//data[column];
            if (val == null) return null;
            indexData.put(header.name, val);
        //}
        return indexData;
    	} catch (Exception e){
    		System.out.println("getIndexData"+e.getMessage());
    		return null;
    	}
    }
    static public long getId(Object[] data) {
    		return (Long)data[0];
    }
   

}
