package com.theruipu.mysql;

import java.sql.SQLException;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			MysqlUtils.InsertTopnIp("ssss","bbbbss",53);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
