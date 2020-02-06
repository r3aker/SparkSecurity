package com.theruipu.mysql;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

public class MysqlUtils {

	public static QueryRunner getQueryRunner() {
		QueryRunner runner = new QueryRunner(MySQLPool.getInstance().getDataSource());
		return runner;
	}

	/**
	 * 
	 * @param tableName
	 *            数据库表名
	 * @param params
	 *            key为字段名，value为字段值
	 * @return 操作成功的行数
	 * @throws SQLException
	 */
	public static int insertData(String tableName, Map<String, Object> params) throws SQLException {

		StringBuffer sb = new StringBuffer();
		StringBuffer columns = new StringBuffer();
		StringBuffer tokens = new StringBuffer();
		for (String key : params.keySet()) {
			columns.append(key).append(",");
			tokens.append("?").append(",");
		}
		columns.delete(columns.length() - 1, columns.length());
		tokens.delete(tokens.length() - 1, tokens.length());
		sb.append("insert into ").append(tableName);
		sb.append("(");
		sb.append(columns);
		sb.append(") values (");
		sb.append(tokens);
		sb.append(")");
		// System.out.println(sb);
		// System.out.println(params.values().toArray());
		QueryRunner runner = getQueryRunner();
		return runner.update(sb.toString(), params.values().toArray());
	}
	// 根据id获取数据查询
	public static List<String>  querystat(String project,String project2,String action, String action2) throws SQLException{
		QueryRunner qr = getQueryRunner();
		  List<String>  bb = qr.query("(select * from statistics where project=? and action=? order by time  desc limit 2) union (select * from statistics where project=? and action=? and time < DATE_SUB(now(), INTERVAL 1 DAY) order by time  desc limit 1)", new BeanListHandler<String>(String.class),project,action,project2,action2);
		  return bb;
	}

	// 批量删除
	// public static void delBooks(String project) throws SQLException {
	// QueryRunner qr = getQueryRunner();
	// }
	// 更新
	public static void Update(String project,String alarmid, String alarmtime, Integer minute,String ip) throws SQLException {
		QueryRunner qr = getQueryRunner();
		qr.update("update detail set alarm_id=? where round((UNIX_TIMESTAMP(?)-UNIX_TIMESTAMP(time))/60)<? and project=? and alarm_id=?", alarmid,
				alarmtime, minute,project,ip);
	}
	//插入到topip的错误数
	public static void InsertTopnIp(String Project,String count,Integer topn) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into topip(project,illustration,number) values(?,?,?)", Project,
				count, topn);
	}
	//根据logtemp插入到detail表中
	public static void InsertSelectDetail(String project,String alarmid) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into detail select null,?,?,now(), url from logtemp where ip =(select ip from alarm where id=?)  and round((UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(stormdt))/60)<20"
					, project,alarmid,alarmid);
	}

	public static void InsertErrorURL(String project,String time,String URL,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into ErrorURL(project,time,URL,Number) values(?,?,?,?)", project,time,URL, number);
	}
	
	
	public static void InsertFiveminuteLoginRanking(String project,String time,String timeip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into FiveminuteLoginRanking(Project,Time,TimeIP,Number) values(?,?,?,?)", project,time,timeip, number);
	}
	
	public static void InsertUserAgentTopIP(String project,String time,String timeip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into UserAgentTopIP(Project,Time,TimeIP,Number) values(?,?,?,?)", project,time,timeip, number);
	}
	
	public static void InsertFiveminuteRegisterRanking(String project,String time,String timeip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into FiveminuteRegisterRanking(Project,Time,TimeIP,Number) values(?,?,?,?)", project,time,timeip, number);
	}
	
	public static void InsertFiveminuteSendmessageRanking(String project,String time,String timeip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into FiveminuteSendmessageRanking(Project,Time,TimeIP,Number) values(?,?,?,?)", project,time,timeip, number);
	}
	public static void InsertVisitRegion(String project,String time,String region,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into VisitRegion(Project,Time,Region,Number) values(?,?,?,?)", project,time,region, number);
	}
	public static void InsertTopIP(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIP(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	public static void InsertTopIPURL(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIPURL(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	public static void InsertTrafficTopIP(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TrafficTopIP(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	public static void InsertTrafficTopURL(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TrafficTopURL(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	public static void InsertTopIPLogin(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIPLogin(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	public static void InsertTopIPLoginFail(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIPLoginFail(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	

	public static void InsertNightLogin(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into NightLogin(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	public static void InsertTopIPRegister(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIPRegister(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	
	public static void InsertTopIPSendMessage(String project,String time,String ip,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopIPSendMessage(project,time,IP,Number) values(?,?,?,?)", project,time,ip, number);
	}
	
	public static void InsertTopURL(String project,String time,String url,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopURL(project,time,URL,Number) values(?,?,?,?)", project,time,url, number);
	}
	
	public static void InsertTopReferURL(String project,String time,String url,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopReferURL(project,time,URL,Number) values(?,?,?,?)", project,time,url, number);
	}
	
	public static void InsertTopEntryUrl(String project,String time,String url,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopEntryURL(project,time,URL,Number) values(?,?,?,?)", project,time,url, number);
	}
	
	
	public static void InsertTopURLNoStatic(String project,String time,String url,Integer number) throws SQLException{
		QueryRunner qr = getQueryRunner();
		qr.update("insert into TopURLNoStatic(project,time,URL,Number) values(?,?,?,?)", project,time,url, number);
	}
	
}
