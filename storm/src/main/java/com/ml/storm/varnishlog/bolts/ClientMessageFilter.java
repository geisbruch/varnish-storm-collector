package com.ml.storm.varnishlog.bolts;


import com.ml.storm.varnishlog.dto.VarnishMessage;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ClientMessageFilter extends MessageFilter{

	@Override
	protected boolean passTuple(Tuple input) {
		return "c".equals(((VarnishMessage)input.getValueByField("message")).connectionType);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","message"));
	}

}
