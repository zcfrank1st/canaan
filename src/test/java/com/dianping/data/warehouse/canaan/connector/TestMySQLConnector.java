package com.dianping.data.warehouse.canaan.connector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.connector.MySQLConnector;

public class TestMySQLConnector extends TestCase {
	public void testExecute() throws FileNotFoundException, IOException,
			ClassNotFoundException, SQLException {
		CanaanConf canaanConf = CanaanConf.getConf();
		canaanConf.loadHiveLogConf();

		MySQLConnector conn = new MySQLConnector(canaanConf.getHiveLogConf()
				.getMysqlConnectParams());
		String sql1 = "DROP TABLE IF EXISTS cyf_test_20121018;"
				+ "CREATE TABLE "
				+ "IF NOT EXISTS cyf_test_20121018 (a INT, b INT)";
		String sql2 = "insert into cyf_test_20121018 values(1,2);";
		conn.connect();
		conn.execute(sql1);
		conn.execute(sql2);
		conn.close();
	}

	public void testExecuteUpdate() throws FileNotFoundException, IOException,
			ClassNotFoundException, SQLException {
		CanaanConf canaanConf = CanaanConf.getConf();
		canaanConf.loadHiveLogConf();
		MySQLConnector conn = new MySQLConnector(canaanConf.getHiveLogConf()
				.getMysqlConnectParams());
		String sql_crt_table = "DROP TABLE IF EXISTS cyf_test_20121019;"
				+ "CREATE TABLE cyf_test_20121019 ( "
				+ "logid INT (11) AUTO_INCREMENT," + "a INT," + "b INT,"
				+ "PRIMARY KEY (logid));";
		String sql2 = "insert into cyf_test_20121019(a,b) values(1,2);";
		String sql3 = "insert into cyf_test_20121019(a,b) values(2,4);";
		conn.connect();
		conn.execute(sql_crt_table);
		assertEquals(1, conn.executeUpdate(sql2));
		assertEquals(2, conn.executeUpdate(sql3));
		conn.close();
	}
}