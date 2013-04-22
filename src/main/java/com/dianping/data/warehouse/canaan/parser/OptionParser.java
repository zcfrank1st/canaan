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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.dianping.data.warehouse.canaan.common.Constants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.dianping.data.warehouse.canaan.exception.CanaanPrintHelpException;
import com.dianping.data.warehouse.canaan.exception.ParamNotSupportException;
import com.dianping.data.warehouse.canaan.util.DateUtils;

/**
 * TODO Comment of OptionParser
 * 
 * @author yifan.cao
 * 
 */
public class OptionParser {
	static CommandLineParser parser;
	static Options options;

	static {
		options = new Options();

		for (String k : Constants.param2DescMapping.keySet()) {
			// for -h(Help):
			if (k.equals(Constants.PARAM_IN_H.toString()))
				options.addOption(Constants.PARAM_IN_H, false, Constants.PARAM_IN_DESC_H);
			// for others:
			else if (k.equals(Constants.PARAM_IN_P.toString()))
                options.addOption(k,false,Constants.param2DescMapping.get(k));
			else
                options.addOption(k, true, Constants.param2DescMapping.get(k));

		}

		// for -E <paramN=valN>
		OptionBuilder.withArgName(Constants.PARAM_IN_DESC_EXT_ARG);
		OptionBuilder.hasArgs(2);
		OptionBuilder.withValueSeparator(Constants.PARAM_IN_EXT_DELIMITER);
		OptionBuilder.withDescription(Constants.PARAM_IN_DESC_EXT);
		Option property = OptionBuilder.create(Constants.PARAM_IN_EXT);
		options.addOption(property);
		parser = new PosixParser();
	}

	/**
	 * @param args
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public static void process(String[] args, Map<String, String> map) throws Exception {

		CommandLine cl = parser.parse(options, args);

		/*
		 * for no args case
		 */
		if (cl.getOptions().length > 0) {
			if (cl.hasOption(Constants.PARAM_IN_H)) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("Options", options);
				throw new CanaanPrintHelpException("Print Help");
			}
            else {

				/*
				 * load common args
				 */
				for (String key : Constants.param2ContextVarMapping.keySet()) {
					String value = cl.getOptionValue(key);

					String var = Constants.param2ContextVarMapping.get(key);
					/*
					 * date to standard date string
					 */
					if (var.equals(Constants.BATCH_COMMON_VARS.BATCH_CAL_DT.toString())) {
						if (value != null) {
							try {
								value = DateUtils.getFormatDateString(value);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								throw new ParamNotSupportException("ERROR_DATE_VALUE_NOTSUPPORT");
							}
						}
					}

					/*
					 * server id => standard server constant
					 */
					if (var.equals(Constants.BATCH_COMMON_VARS.BATCH_SERVER.toString())) {
						if (value != null) {
							String s = Constants.id2serverMapping.get(value);
							if (s == null)
								throw new ParamNotSupportException("ERROR_SERVERID_NOTSUPPORT");
							value = s;
						}
					}

					/*
					 * user type => standard user constant
					 */
					if (var.equals(Constants.BATCH_COMMON_VARS.BATCH_USER.toString())) {
						if (value != null) {
							boolean bUserChk = false;
							for (Constants.BATCH_USERS ut : Constants.BATCH_USERS.values())
								if (value.toUpperCase().equals(ut.toString())) {
									value = ut.toString();
									bUserChk = true;
								}
							if (!bUserChk)
								throw new ParamNotSupportException("ERROR_USER_NOTSUPPORT");
						}
					}

					/*
					 * dol pathname => filename
					 */
					if (var.equals(Constants.BATCH_COMMON_VARS.BATCH_DOL.toString())) {
						if (value != null) {
							try {
								value = value.substring(value.lastIndexOf(File.separator)+1);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								throw new ParamNotSupportException("ERROR_DOL_FILENAME");
							}
						}
					}
					// System.out.println("-" + key + ": " + val);
					map.put(Constants.param2ContextVarMapping.get(key), value);

                }
                /*
                 * parse only option for -p
                */
                if (cl.hasOption(Constants.PARAM_IN_P))
                {
                    map.put(Constants.param2ContextVarMapping.get(Constants.PARAM_IN_P),"T");
                }
                else
                {
                    map.put(Constants.param2ContextVarMapping.get(Constants.PARAM_IN_P),"F");
                }

                /*
                 * for dol str
                 */
                if (cl.hasOption(Constants.PARAM_IN_STR) && cl.hasOption(Constants.PARAM_IN_DOL))
                    throw new ParamNotSupportException("ARGS_CONFILCTED");
                else if (cl.hasOption(Constants.PARAM_IN_STR))
                    map.put(Constants.BATCH_COMMON_VARS.BATCH_DOL_TYPE.toString(),Constants.DOL_TYPE_STR);
                else map.put(Constants.BATCH_COMMON_VARS.BATCH_DOL_TYPE.toString(),Constants.DOL_TYPE_DOL);

				/*
				 * load extended args
				 */
				Properties keys = cl.getOptionProperties(Constants.PARAM_IN_EXT);
				for (Object key : keys.keySet()) {
					map.put(key.toString(), keys.getProperty(key.toString()));
					// System.out.println("-" + Constants.PARAM_IN_EXT + " " +
					// key + "="
					// + keys.getProperty(key.toString()));
				}


			}
		} else {
			throw new ParamNotSupportException("ERROR_NOARGS");
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		while (true) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			try {
				String[] strs = br.readLine().split(" ");
				Map<String, String> map = new HashMap<String, String>();
				OptionParser.process(strs, map);
				for (String k : map.keySet()) {
					System.err.println(k + ":\t" + map.get(k));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
