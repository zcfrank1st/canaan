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
package com.dianping.data.warehouse.canaan.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.log.HiveLogConf;
import com.dianping.data.warehouse.canaan.util.DateUtils;

/**
 * TODO Comment of Context
 * 
 * @author yifan.cao
 */
public class CanaanConf {
	private static CanaanConf conf = null;
	private Map<String, String> optionConf = new HashMap<String, String>();
	private Map<String, String> envConf = new HashMap<String, String>();
	private Properties defaultConf = new Properties();
	private HiveLogConf hiveLogConf = null;
	private DOLite dolite;
	private ArrayList<HiveCMDInfo> hivecmd;
	private String tmpPath = null;
	private Properties canaanProp = null;

	public static CanaanConf getConf() throws FileNotFoundException,
			IOException {
		if (conf == null) {
			conf = new CanaanConf();
		}
		conf.init();
		return conf;
	}
	
	/************
	 * 完成初始化
	 * @throws java.io.FileNotFoundException
	 * @throws java.io.IOException
	 */
	private void init() throws FileNotFoundException, IOException {
		loadEnvConf();
		
		loadDefaultConf(new StringBuilder()
				.append(getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_CONF_DIR
						.toString())).append(File.separator)
				.append(Constants.DEFAULT_CONF_NAME).toString());
	}
	
	/*************
	 * 加载环境变量
	 */
	public void loadEnvConf() {
		Map<String, String> em = System.getenv();
		for (String k : em.keySet()) {
			envConf.put(k.toUpperCase(), em.get(k));
		}

		String home = getEnvConfVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME
				.toString());
		String confsub = getEnvConfVariables(Constants.BATCH_COMMON_VARS.CANAAN_CONF_SUBDIR
				.toString());

		envConf.put(
				Constants.BATCH_COMMON_VARS.CANAAN_CONF_DIR.toString(),
				new StringBuilder().append(home).append(File.separator)
						.append("conf").append(File.separator).append(confsub)
						.toString());
	}
	
	/*************
	 * 加载配置文件变量
	 * @param filePath
	 * @throws java.io.FileNotFoundException
	 * @throws java.io.IOException
	 */
	public void loadDefaultConf(String filePath) throws FileNotFoundException,
			IOException {
		FileInputStream inputFile = new FileInputStream(filePath);
		defaultConf.loadFromXML(inputFile);
		Util.substVars(defaultConf);
		inputFile.close();
	}

	public String getDefaultConfVariables(String key) {
		if (key.equals(Constants.BATCH_COMMON_VARS.BATCH_CAL_DT.toString())) {
			return DateUtils
					.getFormatDateString(Constants.DEFAULT_BATCH_CAL_DT_OFFSET);
		}
		return defaultConf.getProperty(key);
	}

	public String printDefaultConfVariables() {
		StringBuilder sb = new StringBuilder();
		for (Entry<Object, Object> e : defaultConf.entrySet())
			sb.append(e.getKey()).append(": ").append(e.getValue())
					.append("\n");
		return sb.toString();
	}

	public void setOptionConfVariables(String key, String value) {
		optionConf.put(key, value);
	}

	public String getOptionConfVariables(String key) {
		return optionConf.get(key);
	}

	public String getEnvConfVariables(String key) {
		return envConf.get(key);
	}

	public String getCanaanVariables(String key) {
		String value;
		String optionValue = getOptionConfVariables(key);
		String envValue = getEnvConfVariables(key);

		if (optionValue != null) {
			value = optionValue;
		} else if (envValue != null) {
			value = envValue;
		} else {
			String defaultValue = getDefaultConfVariables(key);
			value = defaultValue;
		}
		return value;
	}

	public Properties getCanaanProperties() {
		if (this.canaanProp == null) {
			this.canaanProp = new Properties();
			for (Constants.BATCH_CAL_VARS var : Constants.BATCH_CAL_VARS
					.values()) {
				String key = var.toString();
				String value = getCalVariables(key);
				this.canaanProp.setProperty(key, value);
			}

			for (Object key : defaultConf.keySet()) {
				if (defaultConf.getProperty(key.toString()) != null) {
					this.canaanProp.setProperty(key.toString(),
							defaultConf.getProperty(key.toString()));

				}
			}

			for (Object key : envConf.keySet()) {
				if (envConf.get(key.toString()) != null) {
					this.canaanProp.setProperty(key.toString(),
							envConf.get(key.toString()));
				}
			}

			for (Object key : optionConf.keySet()) {
				if (optionConf.get(key.toString()) != null) {
					this.canaanProp.setProperty(key.toString(),
							optionConf.get(key.toString()));
				}
			}
		}
		return this.canaanProp;
	}

	public HiveLogConf getHiveLogConf() {
		return hiveLogConf;
	}

    public String getHiveTmpPath() {
        if (tmpPath != null)
            return tmpPath;
        else {
            if (getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL
                    .toString()) == null)
                return null;
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName(); // format: "pid@hostname"
            String pid = "0";
            try {
                pid = name.substring(0, name.indexOf('@'));
            } catch (Exception ignored) {
            }

            String tmpDirPath = getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME
                    .toString()) + File.separator + "tmp";
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                tmpDir.mkdir();
            }
            Random ran = new Random(System.currentTimeMillis());
            tmpPath = tmpDirPath
                    + File.separator
                    + getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL
                    .toString()) + "."
                    + Constants.LONG_DF_FOR_FILENAME.format(new Date()) + "_"
                    + ran.nextInt(10000)
                    + "_"
                    + pid;
        }
        return tmpPath;
    }

	@Deprecated
	public void loadHiveLogPath(String logDir, String logFile) {
		if (hiveLogConf == null) {
			hiveLogConf = HiveLogConf.getConf();
		}
		hiveLogConf.setLogDir(logDir);
		hiveLogConf.setLogFile(logFile);
	}

	@Deprecated
	public void loadHiveLogMysqlConnectParams(
			Map<String, String> mysqlConnectParams) {
		if (hiveLogConf == null) {
			hiveLogConf = HiveLogConf.getConf();
		}
		hiveLogConf.setMysqlConnectParams(mysqlConnectParams);
	}

	public void loadHiveLogConf() {
		if (hiveLogConf == null) {
			hiveLogConf = HiveLogConf.getConf();
		}
		hiveLogConf.setLogDir(this
				.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_LOG_DIR
						.toString()));
		hiveLogConf.setLogFile(this.getCanaanVariables(
				Constants.BATCH_COMMON_VARS.BATCH_DOL.toString()).toString());
		Map<String, String> connectParams = new HashMap<String, String>();
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.USERNAME.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.USERNAME
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.PASSWORD.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.PASSWORD
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.HOST.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.HOST
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.PORT.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.PORT
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.DATABASE.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.DATABASE
						.toString()));
		connectParams.put(Constants.MYSQL_CONNECT_PARAMS.SWITCH.toString(),
				getCanaanVariables(Constants.MYSQL_CONNECT_PARAMS.SWITCH
						.toString()));
		hiveLogConf.setMysqlConnectParams(connectParams);
		hiveLogConf.setCaldate(this
				.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_CAL_DT
						.toString()));
		hiveLogConf
				.setTaskid(getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_TASK_ID
						.toString()));
		hiveLogConf
				.setTaskname(getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL
						.toString()));
	}

	public void loadOptionVariables(Map<String, String> map) {
		this.optionConf = map;
	}

	public DOLite getDOLite() {
		return dolite;
	}

	public void setDOLite(DOLite dolite) {
		this.dolite = dolite;
	}

	public ArrayList<HiveCMDInfo> getHivecmd() {
		return hivecmd;
	}

	public void setHivecmd(ArrayList<HiveCMDInfo> hivecmd) {
		this.hivecmd = hivecmd;
	}

	public String getCalVariables(String key) {
		// YYYYMMDD_P1D YYYYMMDD_P1D YYYYMMDD_YESTERDAY YYYYMMDD_DEFAULT_HP_DT
		String value = "";
        if ("HH".equalsIgnoreCase(key))
        {
            String cal_hour =  getCanaanVariables(Constants
                    .BATCH_COMMON_VARS
                    .BATCH_TIMESTAMP
                    .toString());
            return cal_hour == null?"00":cal_hour;
        }
		if ("YYYYMMDD_DEFAULT_HP_DT".equalsIgnoreCase(key))
			return Constants.DEFAULT_HP_DT;

		try {
			String[] v = key.split("_");
			int sign = 1;
			int offset = 0;
			SimpleDateFormat format = Constants.LONG_DF;
			int field = Calendar.DATE;

			if (v.length > 0) {
				String frmt = v[0];
				if ("YYYYMMDD".equalsIgnoreCase(frmt))
					format = Constants.DAY_DF;
				else if ("YYYYMM".equalsIgnoreCase(frmt))
					format = Constants.MONTH_DF;
				else if ("YYYY".equalsIgnoreCase(frmt))
					format = Constants.YEAR_DF;
				else if ("MM".equalsIgnoreCase(frmt))
					format = Constants.MM_DF;
				else if ("DD".equalsIgnoreCase(frmt))
					format = Constants.DD_DF;
			}

			if (v.length > 1) {
				String s = v[1];
				if (s.toUpperCase().endsWith("TODAY")) {
					sign = 1;
					offset = 0;
					return DateUtils.getFormatDateString(
							Constants.DAY_DF.format(new Date()), field, offset
									* sign, format);
				} else if (s.toUpperCase().endsWith("YESTERDAY")) {
					sign = 1;
					offset = -1;
					return DateUtils.getFormatDateString(
							Constants.DAY_DF.format(new Date()), field, offset
									* sign, format);
				} else {
					sign = s.startsWith("P") ? -1 : 1;
					offset = Integer.valueOf(s.replaceAll("[^0-9]", ""));
					if (s.toUpperCase().endsWith("DOWIM"))
						field = Calendar.DAY_OF_WEEK_IN_MONTH;
					else if (s.toUpperCase().endsWith("DOW"))
						field = Calendar.DAY_OF_WEEK;
					else if (s.toUpperCase().endsWith("DOM"))
						field = Calendar.DAY_OF_MONTH;
					else if (s.toUpperCase().endsWith("DOY"))
						field = Calendar.DAY_OF_YEAR;
					else if (s.toUpperCase().endsWith("D"))
						field = Calendar.DATE;
					else if (s.toUpperCase().endsWith("M"))
						field = Calendar.MONTH;
					else if (s.toUpperCase().endsWith("Y"))
						field = Calendar.YEAR;
					else if (s.toUpperCase().endsWith("WOY"))
						field = Calendar.WEEK_OF_YEAR;
				}

			}

			String batch_cal_dt = getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_CAL_DT
					.toString());

			value = DateUtils.getFormatDateString(batch_cal_dt, field, offset
					* sign, format);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return value;
	}

	public Set<String> getExtParamKeys() {
		Set<String> rtn = new HashSet<String>();
		for (String k : optionConf.keySet()) {
			for (Constants.BATCH_COMMON_VARS cv : Constants.BATCH_COMMON_VARS
					.values())
				if (k.equalsIgnoreCase(cv.toString()))
					break;
			rtn.add(k);
		}
		return rtn;
	}
}

class Util {
	private static final String DELIM_START = "${";
	private static final String DELIM_STOP = "}";

	/**
	 * Performs variable substitution for a complete set of properties
	 * 
	 * @param properties
	 *            Set of properties to apply substitution on.
	 * @return Same set of properties with all variables substituted.
	 * @see #substVars(String, String, java.util.Map, java.util.Properties)
	 */
	public static Properties substVars(Properties properties) {
		for (Enumeration propertyKeys = properties.propertyNames(); propertyKeys
				.hasMoreElements();) {
			String name = (String) propertyKeys.nextElement();
			String value = properties.getProperty(name);
			properties.setProperty(name,
					substVars(value, name, null, properties));
		}
		return properties;
	}

	/**
	 * <p>
	 * This method performs property variable substitution on the specified
	 * value. If the specified value contains the syntax
	 * <tt>${&lt;prop-name&gt;}</tt>, where <tt>&lt;prop-name&gt;</tt> refers to
	 * either a configuration property or a system property, then the
	 * corresponding property value is substituted for the variable placeholder.
	 * Multiple variable placeholders may exist in the specified value as well
	 * as nested variable placeholders, which are substituted from inner most to
	 * outer most. Configuration properties override system properties.
	 * </p>
	 * 
	 * @param val
	 *            The string on which to perform property substitution.
	 * @param currentKey
	 *            The key of the property being evaluated used to detect cycles.
	 * @param cycleMap
	 *            Map of variable references used to detect nested cycles.
	 * @param configProps
	 *            Set of configuration properties.
	 * @return The value of the specified string after system property
	 *         substitution.
	 * @throws IllegalArgumentException
	 *             If there was a syntax error in the property placeholder
	 *             syntax or a recursive variable reference.
	 */
	private static String substVars(String val, String currentKey,
			Map<String, String> cycleMap, Properties configProps)
			throws IllegalArgumentException {
		// If there is currently no cycle map, then create
		// one for detecting cycles for this invocation.
		if (cycleMap == null) {
			cycleMap = new HashMap<String, String>();
		}

		// Put the current key in the cycle map.
		cycleMap.put(currentKey, currentKey);

		// Assume we have a value that is something like:
		// "leading ${foo.${bar}} middle ${baz} trailing"

		// Find the first ending '}' variable delimiter, which
		// will correspond to the first deepest nested variable
		// placeholder.
		int stopDelim = val.indexOf(DELIM_STOP);

		// Find the matching starting "${" variable delimiter
		// by looping until we find a start delimiter that is
		// greater than the stop delimiter we have found.
		int startDelim = val.indexOf(DELIM_START);
		while (stopDelim >= 0) {
			int idx = val.indexOf(DELIM_START,
					startDelim + DELIM_START.length());
			if ((idx < 0) || (idx > stopDelim)) {
				break;
			} else if (idx < stopDelim) {
				startDelim = idx;
			}
		}

		// If we do not have a start or stop delimiter, then just
		// return the existing value.
		if ((startDelim < 0) && (stopDelim < 0)) {
			return val;
		}
		// At this point, we found a stop delimiter without a start,
		// so throw an exception.
		else if (((startDelim < 0) || (startDelim > stopDelim))
				&& (stopDelim >= 0)) {
			throw new IllegalArgumentException(
					"stop delimiter with no start delimiter: " + val);
		}

		// At this point, we have found a variable placeholder so
		// we must perform a variable substitution on it.
		// Using the start and stop delimiter indices, extract
		// the first, deepest nested variable placeholder.
		String variable = val.substring(startDelim + DELIM_START.length(),
				stopDelim);

		// Verify that this is not a recursive variable reference.
		if (cycleMap.get(variable) != null) {
			throw new IllegalArgumentException("recursive variable reference: "
					+ variable);
		}

		// Get the value of the deepest nested variable placeholder.
		// Try to configuration properties first.
		String substValue = (configProps != null) ? configProps.getProperty(
				variable, null) : null;
		if (substValue == null) {
			// Ignore unknown property values.
			substValue = System.getenv(variable);
			if (substValue == null) {
				substValue = "";
			}
		}

		// Remove the found variable from the cycle map, since
		// it may appear more than once in the value and we don't
		// want such situations to appear as a recursive reference.
		cycleMap.remove(variable);

		// Append the leading characters, the substituted value of
		// the variable, and the trailing characters to get the new
		// value.
		val = val.substring(0, startDelim) + substValue
				+ val.substring(stopDelim + DELIM_STOP.length(), val.length());

		// Now perform substitution again, since there could still
		// be substitutions to make.
		val = substVars(val, currentKey, cycleMap, configProps);

		// Return the value.
		return val;
	}
}
