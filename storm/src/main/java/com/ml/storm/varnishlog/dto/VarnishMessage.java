package com.ml.storm.varnishlog.dto;

import java.io.Serializable;

public class VarnishMessage implements Serializable {

	public String logSession; 
	public String messageType; 
	public String connectionType;
	public String messageLog;
	public String backendSession = null;
	
	public VarnishMessage(String logSession, String messageType,
			String connectionType, String messageLog) {
		super();
		this.logSession = logSession;
		this.messageType = messageType;
		this.connectionType = connectionType;
		this.messageLog = messageLog;
	}

}
