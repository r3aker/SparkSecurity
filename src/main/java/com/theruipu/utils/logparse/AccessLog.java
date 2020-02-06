package com.theruipu.utils.logparse;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.theruipu.utils.Getconfig;
import com.theruipu.utils.TimeTranslate;

public class AccessLog {
	private String ClientIP;
	private String Domain;
	private Date Time;
	private String Method;
	private String URL;
	private Integer Status;
	private Integer ReponseSize=1500;
	private String Useragent;
	private String Refer;
    public Integer Success=1;
	
	
	public AccessLog(String s) {
		Pattern p = Pattern.compile(Getconfig.Getproperty("weblogrex"));
		
		Matcher m = p.matcher(s);

		if (m.find()) {
			ClientIP = m.group("src");
			String Datetime = "";
			Datetime = m.group("date") + " " + m.group("time").substring(0, 5) + ":00";
		
			Time = TimeTranslate.StringtoDatetime(Datetime);
			URL= m.group("url");
			Status =  new Integer(m.group("status"));
			Domain = m.group("domain");
			ReponseSize = new Integer(m.group("length"));
			Useragent = m.group("agent");
			Refer = m.group("refer");
			Success=2;
		}

	}
	
	
	
	
	
	
	
	
	
	public void setClientIP(String IP) {
		this.ClientIP = IP;
	}

	public void setDomain(String Domain) {
		this.Domain = Domain;
	}

	public void setTime(Date Time) {
		this.Time = Time;
	}

	public void setMethod(String Method) {
		this.Method = Method;
	}

	public void setURL(String URL) {
		this.URL = URL;
	}

	public void setStatus(Integer Status) {
		this.Status = Status;
	}

	public String getClientIP() {
		return ClientIP;
	}

	public String getDomain() {
		return Domain;
	}

	public Date getTime() {
		return Time;
	}

	public String getMethod() {
		return Method;
	}

	public String getURL() {
		return URL;
	}
	
	public String getUserAgent() {
		return Useragent;
	}
	
	public String getRefer() {
		return Refer;
	}

	public Integer getStatus() {
		return Status;
	}
	public Integer getResponseSize() {
		return ReponseSize;
	}
	
	public String getIPUserAgent() {
		return ClientIP+"-"+Useragent;
	}
	
}
