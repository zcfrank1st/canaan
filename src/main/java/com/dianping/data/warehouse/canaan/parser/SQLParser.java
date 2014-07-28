/**
 * Project: canaan
 * 
 * File Created at 2012-9-24
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.data.warehouse.canaan.parser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.connector.MySQLConnector;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.log4j.Logger;

import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;

import com.dianping.data.warehouse.canaan.pedigree.AnalyzeResult;
import com.dianping.data.warehouse.canaan.pedigree.PedigreeAnalyzer;

/**
 * TODO Comment of SQLParser
 * 
 * @author leo.chen
 * 
 */
public class SQLParser {
	private static final Logger LOG = Logger.getLogger(SQLParser.class);

	private DOLite dolite;
	private Map<Integer, AnalyzeResult> pedigreeAnalyzeResult;
	private PedigreeAnalyzer pa;
	private String fileName;
	private String taskId;
	private boolean isDolite;
	private String command;

	public SQLParser(DOLite dolite) {
		this.dolite = dolite;
		this.isDolite = true;
		this.fileName = dolite.getFileName();
		this.taskId = dolite.getTaskId();
		init();
	}

	public SQLParser(String command) {
		this.command = command;
		this.isDolite = false;
		init();
	}

	private void init() {
		pa = PedigreeAnalyzer.getAnalyzer();
		pedigreeAnalyzeResult = new HashMap<Integer, AnalyzeResult>();
	}

	private boolean canParse(String sql) {
		// filter like "set mapred.reduce.tasks=100"
		for(String prefix: Constants.NOSQL_PREFIX1){
			if (sql.trim().toLowerCase().startsWith(prefix)) {
				return false;
			}
		}
		for(String prefix:Constants.NOSQL_PREFIX2){
			if (sql.trim().toLowerCase().startsWith(prefix)) {
				return false;
			}
		}

        // temp solution for add temporary function
        String regEx="create\\s+temporary\\s+function"; //表示一个或多个@
        Pattern pat=Pattern.compile(regEx);
        Matcher mat=pat.matcher(sql.trim().toLowerCase());
        if (mat.find()) {
            return false;
        }
        return true;
	}

	public void parse() throws ParseException {
		if (isDolite) {
			for (int line = 1; line <= dolite.size(); line++) {
				if (canParse(dolite.get(line))) {
					pedigreeAnalyzeResult.put(line,
							pa.analyze(dolite.get(line)));

				}
			}

		} else {
			if (canParse(this.command))
				pedigreeAnalyzeResult.put(0, pa.analyze(this.command));
		}
	}

	public String toString() {
		StrBuilder sb = new StrBuilder("File Name:" + this.fileName + "\n");
		sb.append("task_id:" + this.taskId + "\n");
		for (Entry<Integer, AnalyzeResult> entry : pedigreeAnalyzeResult
				.entrySet()) {
			sb.append("line" + entry.getKey() + ":"
					+ entry.getValue().toString());
		}
		return sb.toString();

	}

	private String getSchema(String name) {
		return name.split(".").length == 2 ? name.split(".")[0] : null;

	}

	private String getTableName(String name) {
		return name.split(".").length == 2 ? name.split(".")[1] : name;
	}

	public void persist() throws FileNotFoundException, IOException,
			ClassNotFoundException, SQLException {
		CanaanConf conf = CanaanConf.getConf();
		Map<String, String> connectParams = new HashMap<String, String>();
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.USERNAME.toString(),
				conf.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.USERNAME
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.PASSWORD.toString(),
				conf.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.PASSWORD
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.HOST.toString(), conf
				.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.HOST
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.PORT.toString(), conf
				.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.PORT
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.DATABASE.toString(),
				conf.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.DATABASE
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.SWITCH.toString(),
				conf.getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.SWITCH
						.toString()));
		MySQLConnector mysqlConnector = new MySQLConnector(connectParams);
		mysqlConnector.connect();

		String sql = "insert into pedigree_analyze_result"
				+ "(task_id,file_name,line_num,parent_schema,parent_tab,child_schema,child_tab)"
				+ "values(?,?,?,?,?,?,?)";
		PreparedStatement pstmt;
		for (Entry<Integer, AnalyzeResult> entry : pedigreeAnalyzeResult
				.entrySet()) {
			int lineNum = entry.getKey();
			for (String child : entry.getValue().getChildTabs()) {
				for (String parent : entry.getValue().getParentTabs()) {
					pstmt = mysqlConnector.getPstmt(sql);
					if (pstmt != null) {
						if (taskId == null) {
							pstmt.setNull(1, java.sql.Types.INTEGER);
						} else {
							pstmt.setInt(1, Integer.parseInt(taskId));
						}
						pstmt.setString(2, fileName);
						pstmt.setInt(3, lineNum);
						pstmt.setString(4, getSchema(parent));
						pstmt.setString(5, getTableName(parent));
						pstmt.setString(6, getSchema(child));
						pstmt.setString(7, getTableName(child));
						LOG.info(pstmt.toString());
						mysqlConnector.executeUpdate(pstmt);
					}
				}

			}

		}
		mysqlConnector.close();
	}

}
