package com.ml.storm.varnishlog.bolts;


import com.ml.storm.varnishlog.dto.VarnishMessage;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BackendMessageFilter extends MessageFilter{

	@Override
	protected boolean passTuple(Tuple input) {
		VarnishMessage message = ((VarnishMessage)input.getValueByField("message"));
		String type = message.connectionType;
		return "b".equals(type) || "Backend".equals(message.messageType);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","message"));
	}

}
