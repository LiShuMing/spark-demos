package com.netease.spark.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author : lishuming
 */
public class Utils {
	private final static Logger LOGGER = LoggerFactory.getLogger(Utils.class);

	public static boolean checkFileExists(String path) {
		File f = new File(path);
		return f.exists();
	}

	public static Properties getPropertiesFromPath(String path) throws Exception {
		return getPropertiesFromFile(new File(path));
	}

	public static Properties getPropertiesFromFile(File f) throws FileNotFoundException {
		if (!f.exists()) {
			throw new FileNotFoundException("config file not exists");
		}
		if (!f.isFile()) {
			throw new FileNotFoundException("path not a file");
		}

		Properties properties = new Properties();
		InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8);
		try {
			properties.load(inputStreamReader);
		} catch (Exception e) {
			LOGGER.warn("load config properties failed:", e);
		} finally {
			try {
				inputStreamReader.close();
			} catch (Exception e) {
				LOGGER.warn("close input reaeder failed:", e);
			}
		}

		return properties;
	}

}
