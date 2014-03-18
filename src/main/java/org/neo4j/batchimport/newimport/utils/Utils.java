package org.neo4j.batchimport.newimport.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;

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

	public static void arrayCopy(int[][] aSource, int[][] aDestination) {
		for (int i = 0; i < aSource.length; i++) {
			System.arraycopy(aSource[i], 0, aDestination[i], 0, aSource[i].length);
		}
	}

	public static String getMaxIds(NeoStore neoStore){
		long[] highIds = new long[4];
		highIds[0] = neoStore.getPropertyStore().getHighId();
		highIds[1] = neoStore.getNodeStore().getHighId();
		highIds[2] = neoStore.getRelationshipStore().getHighId();
		highIds[3] = neoStore.getLabelTokenStore().getHighId();
		return ("Property["+highIds[0]+"] Node["+highIds[1]+"] Relationship["+highIds[2]+"] Label["+highIds[3]+"]");
	}

	public static BufferedReader createFileBufferedReader(File file) {
		try {
			final String fileName = file.getName();
			if (fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
				InputStreamReader inp = new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)),Constants.BUFFERED_READER_BUFFER));
				return new BufferedReader(inp);
			}
			final FileReader fileReader = new FileReader(file);
			return new BufferedReader(fileReader,Constants.BUFFERED_READER_BUFFER);
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

}
