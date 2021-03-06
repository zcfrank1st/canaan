package com.dianping.data.warehouse.canaan.dolite;

public interface DOLite extends Iterable<String> {
	public abstract String get(int index);

	public abstract String getFileName();
	public abstract String getTaskId();
	public abstract int indexOf(String statement);

	public abstract int size();
}