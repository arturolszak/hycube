package net.hycube.utils;

import java.io.File;
import java.util.StringTokenizer;

/**
 * A class containing useful utility methods relating to files.
 */
public class FileUtils {

	/**
	 * Returns a reference to a file with the specified name that is located
	 * somewhere on the classpath.
	 */
	public static File findFileOnClassPath(final String fileName) {


		final String classpath = System.getProperty("java.class.path");

		final String pathSeparator = System.getProperty("path.separator");


		final StringTokenizer tokenizer = new StringTokenizer(classpath, pathSeparator);


		while (tokenizer.hasMoreTokens()) {





			final String pathElement = tokenizer.nextToken();



			final File directoryOrJar = new File(pathElement);


			final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();



			if (absoluteDirectoryOrJar.isFile()) {


				final File target = new File(absoluteDirectoryOrJar.getParent(), fileName);


				if (target.exists()) {



					return target;


				}


			} else {


				final File target = new File(directoryOrJar, fileName);


				if (target.exists()) {



					return target;


				}


			}


		}

		return null;

	}
}