package com.theruipu.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class Geturls {
	

	static Properties props = new Properties();

	static {
		try {
			props.load(Getconfig.class.getResourceAsStream("/Urls.properties"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public static List<String> getkeylist(String key) {
		
		Set<String> c= new HashSet<String>();
		List<String> list = Arrays.asList(props.getProperty(key).split(","));
		
		return list;
	}
	
	public static void main(String[] args) throws Exception {  
		  
		
    }
}
