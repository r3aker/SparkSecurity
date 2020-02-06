package com.theruipu.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Getconfig {

	static Properties props = new Properties();

	static {
		try {
			props.load(Getconfig.class.getResourceAsStream("/Config.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String Getproperty(String key) {

		return props.getProperty(key);
	}

}
