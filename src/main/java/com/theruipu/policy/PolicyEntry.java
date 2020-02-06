package com.theruipu.policy;

import java.io.IOException;
import java.sql.SQLException;

import com.theruipu.policy.login.FiveminuteLoginRanking;
import com.theruipu.policy.login.NightTopLoginIP;
import com.theruipu.policy.login.TopLoginFailIP;
import com.theruipu.policy.login.TopLoginIP;
import com.theruipu.policy.register.FiveminuteRegisterRanking;
import com.theruipu.policy.register.TopIPRegister;
import com.theruipu.policy.sendmessage.FiveminuteSendmessageRanking;
import com.theruipu.policy.sendmessage.TopIPSendMessage;
import com.theruipu.policy.topip.TopIP;
import com.theruipu.policy.topip.TopIPURL;
import com.theruipu.policy.topip.TrafficTopIP;
import com.theruipu.policy.topurl.ErrorURLNoStaticFileRanking;
import com.theruipu.policy.topurl.TOPReferUrl;
import com.theruipu.policy.topurl.TOPUrl;
import com.theruipu.policy.topurl.TOPUrlNoStaticFile;
import com.theruipu.policy.topurl.TopEntryUrl;
import com.theruipu.policy.traffic.TrafficTopURL;
import com.theruipu.policy.useragent.UseragentTopIPRanking;

public class PolicyEntry {
	
	public static void main(String[] args) throws SQLException, IOException {
		
		//ErrorURLNoStaticFileRanking aaa=new ErrorURLNoStaticFileRanking();
		//aaa.Aaaaa();
		
		FiveminuteLoginRanking FiveminuteLogin= new FiveminuteLoginRanking();
		FiveminuteLogin.Count();
		
		NightTopLoginIP IpUrl= new NightTopLoginIP();
		IpUrl.Count();
		
		TopLoginFailIP  TopLoginFail= new TopLoginFailIP();
		TopLoginFail.Count();
		
		TopLoginIP  TopLogin= new TopLoginIP();
		TopLogin.Count();
		
		FiveminuteRegisterRanking  FiveminuteRegister= new FiveminuteRegisterRanking();
		FiveminuteRegister.Count();
		
		TopIPRegister  TopIPReg= new TopIPRegister();
		TopIPReg.Count();
		
		FiveminuteSendmessageRanking  FiveminuteSendmess= new FiveminuteSendmessageRanking();
		FiveminuteSendmess.Count();
		
		TopIPSendMessage  TopIPSendMess= new TopIPSendMessage();
		TopIPSendMess.Count();
		
		TopIP  Topip= new TopIP();
		Topip.Count();
		
		TopIPURL  TopIPurl= new TopIPURL();
		TopIPurl.Count();
		
		TrafficTopIP  TrafficTopip= new TrafficTopIP();
		TrafficTopip.Count();
		
		ErrorURLNoStaticFileRanking  ErrorURLNoStaticFile= new ErrorURLNoStaticFileRanking();
		ErrorURLNoStaticFile.Count();
		
		TopEntryUrl  TopEntryUrl1= new TopEntryUrl();
		TopEntryUrl1.Count();
		
		TOPReferUrl  TOPReferUrl1= new TOPReferUrl();
		TOPReferUrl1.Count();
		
		TOPUrl  TOPUrl1= new TOPUrl();
		TOPUrl1.Count();
		
		TOPUrlNoStaticFile  TOPUrlNoStaticFile1= new TOPUrlNoStaticFile();
		TOPUrlNoStaticFile1.Count();
		
		TrafficTopURL  TrafficTopURL1= new TrafficTopURL();
		TrafficTopURL1.Count();
		
		UseragentTopIPRanking  UseragentTopIP= new UseragentTopIPRanking();
		UseragentTopIP.Count();
		
	
		
		
	}

}
