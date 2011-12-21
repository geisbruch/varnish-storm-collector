package com.ml.storm.varnishlog.dto;

public class VarnishLog {

	public String message;
	public String server;
	public String session;
	
	public VarnishLog(String host, String session, String line) {
		this.message = line;
		this.server = host;
		this.session = session;
	}

}
