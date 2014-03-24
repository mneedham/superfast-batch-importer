package org.neo4j.batchimport.newimport.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;

import org.neo4j.batchimport.NewImporter;
import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;


public class Utils {

	public static String memoryStats(){
		Runtime runtime = Runtime.getRuntime();
		StringBuilder str = new StringBuilder();        
		str.append("Memory stats[MB]");
		str.append("[Used:"+ (runtime.totalMemory() - runtime.freeMemory()) / Constants.mb);
		str.append("][Free:" + runtime.freeMemory() / Constants.mb);
		str.append("][Total:" + runtime.totalMemory() / Constants.mb);
		str.append("[]Max:" + runtime.maxMemory() / Constants.mb+"]");
		return str.toString();
	}

	public static void SystemOutPrintln(String out){
		System.out.println(out);
	}

	public static String getCurrentTimeStamp(){
		Timestamp ts = new Timestamp(System.currentTimeMillis());
        return("["+ts.toString()+"]");
	}
	public static void arrayCopy(int[][] aSource, int[][] aDestination) {
		for (int i = 0; i < aSource.length; i++) {
			System.arraycopy(aSource[i], 0, aDestination[i], 0, aSource[i].length);
		}
	}
	
	public static String getMaxIds(NeoStore neoStore){
		return getMaxIds(neoStore, false);
	}
	static long diskOld = 0;
	public static String getMaxIds(NeoStore neoStore, boolean writeRate){
		long[] highIds = new long[4];
		long rate = 0; long diskNew = 0;
		highIds[0] = neoStore.getPropertyStore().getHighId();
		highIds[1] = neoStore.getNodeStore().getHighId();
		highIds[2] = neoStore.getRelationshipStore().getHighId();
		highIds[3] = neoStore.getLabelTokenStore().getHighId();
		if (writeRate){
			diskNew = (highIds[0]) * neoStore.getPropertyStore().RECORD_SIZE +
						(highIds[1]) * neoStore.getNodeStore().RECORD_SIZE +
						(highIds[2]) * neoStore.getRelationshipStore().RECORD_SIZE +
						(highIds[3]) * neoStore.getLabelTokenStore().getRecordSize();
			rate = (diskNew - diskOld)/(Constants.mb * (Constants.progressPollInterval/1000));
			diskOld = diskNew;
		}
		if (writeRate)
			return ("Property["+highIds[0]+"] Node["+highIds[1]+"] Relationship["+highIds[2]+"] Label["+highIds[3]+"] Disk["+diskNew/Constants.mb+" mb] Rate["+rate+" mb/sec]");	
		return ("Property["+highIds[0]+"] Node["+highIds[1]+"] Relationship["+highIds[2]+"] Label["+highIds[3]+"]");
	}

	public static long getTotalIds(NeoStore neoStore){
		long[] highIds = new long[4];
		highIds[0] = neoStore.getPropertyStore().getHighId();
		highIds[1] = neoStore.getNodeStore().getHighId();
		highIds[2] = neoStore.getRelationshipStore().getHighId();
		highIds[3] = neoStore.getLabelTokenStore().getHighId();
		return (highIds[0]+highIds[1]+highIds[2]+highIds[3]);
	}

	public static Reader createFileReader(File file) {
		try {
			final String fileName = file.getName();
			if (fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                return new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)), Constants.BUFFERED_READER_BUFFER));
			}
			final FileReader fileReader = new FileReader(file);
            return new BufferedReader(fileReader, Constants.BUFFERED_READER_BUFFER);
		} catch(Exception e) {
			throw new IllegalArgumentException("Error reading file "+file+" "+e.getMessage(),e);
		}
	}

	public static String getVersionfinal (Class thisClass) {
		String version = null;
		String shortClassName = thisClass.getName().substring(thisClass.getName().lastIndexOf(".") + 1);
		try {
			ClassLoader cl = thisClass.getClassLoader();
			String threadContexteClass = thisClass.getName().replace('.', '/');
			URL url = cl.getResource(threadContexteClass + ".class");
			if ( url == null ) {
				version = shortClassName + " $ (no manifest)";
			} else {
				String path = url.getPath();
				String jarExt = ".jar";
				int index = path.indexOf(jarExt);
				SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				if (index != -1) {
					String jarPath = path.substring(0, index + jarExt.length());
					File file = new File(jarPath);
					String jarVersion = file.getName();
					JarFile jarFile = new JarFile(new File(new URI(jarPath)));
					JarEntry entry = jarFile.getJarEntry("META-INF/MANIFEST.MF");
					version = shortClassName + " $ " + jarVersion.substring(0, jarVersion.length()
							- jarExt.length()) + " $ "
							+ sdf.format(new Date(entry.getTime()));
					jarFile.close();
				} else {
					File file = new File(path);
					version = shortClassName + " $ " + sdf.format(new Date(file.lastModified()));
				}
			}
		} catch (Exception e) {
			version = shortClassName + " $ " + e.toString();
		}
		return version;
	}

	public static String getCodeLocation(){
		return getCodeLocation(true, 0, null);
	}
	public static String getCodeLocation(boolean withLineNo, int depth){
		return getCodeLocation(withLineNo, depth, null);
	}
	public static String getCodeLocation(boolean withLineNo, int depth, String tag){
		long curTime = (System.currentTimeMillis() - NewImporter.startImport)/1000;
		StackTraceElement[] stack = Thread.currentThread().getStackTrace();
		StringBuilder returnVal = new StringBuilder();
		String[] className = stack[depth].getClassName().split("\\.");
		returnVal.append(className[className.length-1]+"."+stack[depth].getMethodName());
		if (withLineNo)
			returnVal.append("."+stack[depth].getLineNumber());
		returnVal.append(":"+curTime);
		if (tag != null)
			returnVal.append(":"+tag);
		return returnVal.toString();
	}

}
