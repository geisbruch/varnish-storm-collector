package com.ml.storm.varnishlog.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ml.storm.varnishlog.dto.VarnishMessage;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TimeCacheMap;

public class LogCollector implements IBasicBolt {

	TimeCacheMap<Values, List<VarnishMessage>> messages;
	
	@Override
	public void cleanup() {}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String host = input.getStringByField("host");
		String session = input.getStringByField("session");
		Values key = new Values(host,session);
		VarnishMessage message = (VarnishMessage) input.getValueByField("message");
		if(!messages.containsKey(key)){
			messages.put(key, new ArrayList<VarnishMessage>());
		}
		List<VarnishMessage> list = messages.get(key);
		list.add(message);
		
		if(isListFinished(list)){
			collector.emit(new Values(host,session,list));
			messages.remove(key);
		}
		
		
	}

	private boolean isListFinished(List<VarnishMessage> list) {
		boolean reqEnd = false;
		boolean hasBackend = false;
		boolean backendFinished = true;
		
		reqEnd = hasType(list,"ReqEnd");
		hasBackend = hasType(list, "Backend");
		if(hasBackend)
			backendFinished = hasType(list, "BackendReuse") || hasType(list,"BackendClose");
		
		return reqEnd && (!hasBackend || (hasBackend && backendFinished));
	}

	private boolean hasType(List<VarnishMessage> list, String type) {
		for(VarnishMessage m : list)
			if(type.equals(m.messageType))
				return true;
		return false;
	}


	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		int cacheTime = stormConf.containsKey("mapCache")?(Integer)stormConf.get("mapCache"):60;
		messages = new TimeCacheMap<Values, List<VarnishMessage>>(cacheTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","messageList"));
	}

}
