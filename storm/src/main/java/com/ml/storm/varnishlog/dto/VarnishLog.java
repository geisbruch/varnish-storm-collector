package com.ml.storm.varnishlog.dto;

public class VarnishLog {

	public String message;
	public String server;
	
	public VarnishLog(String host, String line) {
		this.message = line;
		this.server = host;
	}

}
