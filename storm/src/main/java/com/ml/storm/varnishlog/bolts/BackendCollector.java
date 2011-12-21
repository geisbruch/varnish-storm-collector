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

public class BackendCollector implements IBasicBolt {

	TimeCacheMap<Values, List<VarnishMessage>> messages;
	TimeCacheMap<Values, String> clientSessions;
	
	
	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String host = input.getStringByField("host");
		String session = input.getStringByField("session");
		Values key = new Values(host,session);
		VarnishMessage message = (VarnishMessage) input.getValueByField("message");
		
		if("Backend".equals(message.messageType)){
			String clientSession = message.logSession;
			clientSessions.put(key, clientSession);
		}else{
			message.backendSession = message.logSession;
			message.logSession = null;
			if(!messages.containsKey(key)){
				messages.put(key, new ArrayList<VarnishMessage>());
			}
			messages.get(key).add(message);
		}
		List<VarnishMessage> messageList = messages.get(key);
		if(isFinished(messageList) && clientSessions.containsKey(key)){
			String clientSession = clientSessions.get(key);
			for(VarnishMessage m : messageList){
				m.logSession = clientSession;
				collector.emit(new Values(host,clientSession,m));
			}
			messages.remove(key);
			clientSessions.remove(key);
		}
	}

	private boolean isFinished(List<VarnishMessage> messageList) {
		if(messageList == null)
			return false;
		for(VarnishMessage message : messageList){
			if("BackendReuse".equals(message.messageType) || "BackendClose".equals(message.messageType)){
				return true;
			}
		}
		return false;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		int cacheTime = stormConf.containsKey("mapCache")?(Integer)stormConf.get("mapCache"):60;;
		messages = new TimeCacheMap<Values, List<VarnishMessage>>(cacheTime);
		clientSessions = new TimeCacheMap<Values, String>(cacheTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","message"));
	}

}
