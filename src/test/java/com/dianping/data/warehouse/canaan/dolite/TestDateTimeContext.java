package com.dianping.data.warehouse.canaan.dolite;

import org.joda.time.DateTime;
import junit.framework.TestCase;

public class TestDateTimeContext extends TestCase {
	public void testXX() {
		DateTime dt = new DateTime();
		// dt.plusDays(1);
		System.out.println(dt.minusDays(-5).getDayOfMonth());
		System.out.println(dt.plusMonths(1).dayOfWeek().withMaximumValue().toString("yyyy-MM-dd"));
		System.out.println(dt.plusMonths(1).dayOfMonth().withMinimumValue());
	}

}
