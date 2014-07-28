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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.exception.CanaanPrintHelpException;

/**
 * TODO Comment of OptionParser
 * @author yifan.cao
 *
 */
public class TestOptionParser extends TestCase {
	String ln = "============================================================================================================";
	public void testNoArgs() throws Exception {
		String str = new String("canaan.jar");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
		try {
			OptionParser.process(args,map);
			assertEquals(true,false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertEquals(true,true);
		}
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
		System.out.println(ln);
    }
	public void testHelp() throws Exception {
		String str = new String("canaan.jar -h");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        try {
			OptionParser.process(args,map);
			for (String k : map.keySet()) {
				System.out.println(k + ":\t" + map.get(k));
			}
		} catch (CanaanPrintHelpException e) {
			
		}
		System.out.println();
        System.out.println(ln);
        assertEquals(true,true);
    }
	public void testAllArgs() throws Exception {	
		String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -d20120902 -s5 -u dwdev -Eparam1=val1 -Eparam2=val2");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        OptionParser.process(args,map);
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
        System.out.println(ln);
        assertEquals(true,true);
    }
	public void testArgsNoExt() throws Exception {	
		String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -u dwdev -d20120902 -s5");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        OptionParser.process(args,map);
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
        System.out.println(ln);
        assertEquals(true,true);
    }
	
	public void testArgsWrongServer() throws Exception {	
		String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -d20120902 -s2");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        try {
			OptionParser.process(args,map);
			assertEquals(true,false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertEquals(true,true);
		}
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
        System.out.println(ln);
    }
	
	public void testArgsWrongUser() throws Exception {	
		String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -d20120902 -s5 -u dwbi");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        try {
			OptionParser.process(args,map);
			assertEquals(true,false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertEquals(true,true);
		}
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
        System.out.println(ln);
    }
	public void testArgsWrongDate() throws Exception {	
		String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -d201209");
		String args[] = str.split(" ");
		System.out.println(str);
		System.out.println(ln);
		Map<String,String> map = new HashMap<String,String>();
        try {
			OptionParser.process(args,map);
			assertEquals(true,false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertEquals(true,true);
		}
		for (String k : map.keySet()) {
			System.out.println(k + ":\t" + map.get(k));
		}
		System.out.println();
        System.out.println(ln);
    }
    public void testPrint() throws Exception {
        String str = new String("canaan.jar -c DWCaculator -dol dwdev.tg.dpmid_tg_test -d20120902 -Eparam1=val1 -Eparam2=val2 -o");
        String args[] = str.split(" ");
        System.out.println(str);
        System.out.println(ln);
        Map<String,String> map = new HashMap<String,String>();
        OptionParser.process(args,map);
        for (String k : map.keySet()) {
            System.out.println(k + ":\t" + map.get(k));
        }
        System.out.println();
        System.out.println(ln);
        assertEquals(true,true);
    }

    public void testHH() throws Exception {
        String str = new String("canaan.jar -dol test -t 1393570800");
        String args[] = str.split(" ");
        System.out.println(str);
        System.out.println(ln);
        Map<String,String> map = new HashMap<String,String>();
        OptionParser.process(args,map);
        for (String k : map.keySet()) {
            System.out.println(k + ":\t" + map.get(k));
        }
        System.out.println();
        System.out.println(ln);
        assertEquals(true,true);
    }
}
