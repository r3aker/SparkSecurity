package com.theruipu.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Getlogdir {
	
	public static String getdir() {
		Calendar cal=Calendar.getInstance();
		//System.out.println(Calendar.DATE);//5
		cal.add(Calendar.DATE,-1);
		Date time=cal.getTime();
		
		String times = new SimpleDateFormat("yyyyMM/dd").format(time);
		String logdir= "/biz/security/"+times+"/"+Getconfig.Getproperty("prodlogdir");
		 
		
		return  logdir;
	}
	
	public static void main(String[] args) throws Exception {  
		  
    	
		System.out.println(Getlogdir.getdir());
    } 
	
	
}
