package com.dianping.data.warehouse.canaan.dolite;

/**
 * 
 * @author leo.chen
 */
public interface DOLiteFactory {
	/**
	 * Produce HQLite
     *
     * @param fileName
	 * @param str
	 * @return
	 * @throws Exception
	 * 
	 */
	  public abstract DOLite produce(String fileName,String str)
			    throws Exception;
}
