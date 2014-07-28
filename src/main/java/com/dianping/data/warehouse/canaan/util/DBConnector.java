package com.dianping.data.warehouse.canaan.util;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.LionException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sunny on 14-7-28.
 */
public class DBConnector {

//    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
//    private static final String DATABASE_URL = "jdbc:mysql://10.1.77.22:3306/dianpingdw_beta";
//    private static final String USERNAME = "aspnet_dianping";
//    private static final String PASSWORD = "";

    Connection connection = null;
    Statement statement = null;

    public DBConnector() {
        ConfigCache configCache = null;
        String driver = null;
        String url = null;
        String username = null;
        String password = null;
        try {
            configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
            driver = configCache.getProperty("galaxy.JDBC.driver");
            url = configCache.getProperty("galaxy.JDBC.url");
            username = configCache.getProperty("galaxy.JDBC.username");
            password = configCache.getProperty("galaxy.JDBC.password");
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username,
                    password);
            statement = connection.createStatement();
        } catch (LionException e) {
            //logger.error("lion get aclAddress fails");
        } catch (Exception e) {
            e.printStackTrace();
            // System.exit(1);
        }
    }

    public List<ExceptionAlertDO> getExceptionAlertsByProduct(String productName) {
        List<ExceptionAlertDO> exceptionAlertDOs = new ArrayList<ExceptionAlertDO>();
        try {
            String sql = "select id,product,description,oncall from etl_exception_cfg where product = '" + productName + "'";
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                String product = rs.getString(2);
                String description = rs.getString(3);
                String oncall = rs.getString(4);
                exceptionAlertDOs.add(new ExceptionAlertDO(id, product, description, oncall));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return exceptionAlertDOs;
    }

}

