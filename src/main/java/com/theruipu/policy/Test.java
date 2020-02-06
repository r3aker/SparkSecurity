package com.theruipu.policy;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.theruipu.policy.login.FiveminuteLoginRanking;

/*
 * 
 * 
 */

public class Test  {

	public static void main(String[] args) throws IOException, SQLException {

		FiveminuteLoginRanking FiveminuteLogin= new FiveminuteLoginRanking();
		FiveminuteLogin.Count();
	}

}
