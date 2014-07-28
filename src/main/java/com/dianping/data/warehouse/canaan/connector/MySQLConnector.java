package com.dianping.data.warehouse.canaan.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.dianping.data.warehouse.canaan.common.Constants;
import org.apache.log4j.Logger;

public class MySQLConnector {
    private static final Logger logger = Logger.getLogger(MySQLConnector.class);
    private Connection conn = null;
    private String jdbcurl;
    private String username;
    private String password;
    private String jdbcswitch;

    public MySQLConnector(Map<String, String> param) {
        jdbcurl = "jdbc:mysql://"
                + param.get(Constants.MYSQL_CONNECT_PARAMS.HOST.toString())
                + ":"
                + param.get(Constants.MYSQL_CONNECT_PARAMS.PORT.toString())
                + "/"
                + param.get(Constants.MYSQL_CONNECT_PARAMS.DATABASE.toString());
        username = param.get(Constants.MYSQL_CONNECT_PARAMS.USERNAME
                .toString());
        password = param.get(Constants.MYSQL_CONNECT_PARAMS.PASSWORD
                .toString());
        jdbcswitch = param.get(Constants.MYSQL_CONNECT_PARAMS.SWITCH
                .toString());
    }

    public void connect() throws ClassNotFoundException {
        if ((jdbcswitch == null) ||  (jdbcswitch.equals("off")))
            return;
        Class.forName("com.mysql.jdbc.Driver");
        try {
            conn = DriverManager.getConnection(jdbcurl, username, password);
        } catch (SQLException e) {
            logger.warn("Mysql log failed");
        }
    }

    @Deprecated
    public void execute(String sql) throws ClassNotFoundException {
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")))
            return;
        try {
            Statement stmt = conn.createStatement();
            String newsql = null;
            if (sql.endsWith(";"))
                newsql = sql.substring(0, sql.length() - 1);
            else
                newsql = sql;
            String[] sqllist = newsql.split(";");
            for (int i = 0; i < sqllist.length; i++) {
                stmt.execute(sqllist[i]);
            }
            stmt.close();
        } catch (SQLException e) {
            logger.warn("Mysql log failed");
        }
    }

    public long executeUpdate(String sql) throws ClassNotFoundException {
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")))
            return 1;
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            return executeUpdate(pstmt);
        } catch (SQLException e) {
            logger.warn("Mysql log failed");
        }
            return 0;
    }

    public long executeUpdate(PreparedStatement pstmt) throws ClassNotFoundException {
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")) || (pstmt == null))
            return 0;
        try {
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                int id = rs.getInt(1);
                return id;
            }
        } catch (SQLException e) {
            logger.warn("Mysql log failed");
        }
        return 0;
    }

    public PreparedStatement getPstmt(String sql) throws ClassNotFoundException {
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")))
            return null;
        try {
            return conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            logger.warn("Mysql log failed");
        }
        return null;
    }

    public void close() {
        // close the connetion
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")))
            return;
        try {
            conn.close();
        } catch (SQLException e) {
            logger.warn("Connection has already closed.");
        }
    }


    private boolean checkActive() {
        if ((jdbcswitch == null) || (jdbcswitch.equals("off")))
            return false;
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("select 1;");
        } catch (SQLException e) {
            try {
                conn = DriverManager.getConnection(jdbcurl, username, password);
            } catch (SQLException e1) {
                return false;
            }
        }
        return true;
    }
}
