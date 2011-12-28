package com.ml.storm.varnishlog.bolts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.ml.storm.varnishlog.dto.VarnishMessage;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VarnishMessageCreator implements IBasicBolt {

	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String host = input.getStringByField("host");
		String session = input.getStringByField("session");
		List<VarnishMessage> messageList = (List<VarnishMessage>) input.getValueByField("messageList");
		
		Map<String, Object> obj = new HashMap<String, Object>();
		obj.put("hasBackend", false);
		/**
		 * For each message check convert type and add to a map
		 */
		for(VarnishMessage message : messageList){
			
			String objName = getObjByCon(message.connectionType,message.messageType);
			
			if(message.messageType.toLowerCase().contains("header")){
				objName = objName+"s";
				/**
				 * if the message it's a header we will create an other map
				 */
				if(!obj.containsKey(objName)){
					obj.put(objName, new HashMap<String, Object>());
				}
				Map<String, Object> map = (Map<String, Object>)obj.get(objName);
				addHeader(message,map);
			}else if(message.messageType.startsWith("VCL_")){
				/**
				 * If it's a vcl we will use a list 
				 */
				objName = getObjByCon(message.connectionType,"VCL");
				if(!obj.containsKey(objName)){
					obj.put(objName, new ArrayList<String>());
				}
				((List<String>)obj.get(objName)).add(message.messageType.split("_")[1]+" "+message.messageLog);
			}else{
				/**
				 * Else assume string
				 */
				
				if("Backend".equals(message.messageType)){
					obj.put("hasBackend", true);
				}else{
					obj.put(objName, message.messageLog);
				}
			}
		}
		obj.put("date", getDate());
		obj.put("host", host);
		obj.put("session", session);
		collector.emit(new Values(host,session,obj));
	}


	private String getDate() {
		return format.format(Calendar.getInstance().getTime());
	}

	private void addHeader(VarnishMessage message, Map<String, Object> map) {
		int idx = message.messageLog.indexOf(":");
		if(idx!=-1){
			String headerName = message.messageLog.substring(0,idx);
			String headerContent = "";
			if(message.messageLog.length() >= idx){
				headerContent = message.messageLog.substring(idx+1);
			}
			map.put(headerName, headerContent);
		}
	}

	private String getObjByCon(String connectionType, String messageType) {
		if("c".equals(connectionType.toLowerCase()))
			return "client"+messageType;
		return "backend"+messageType;
	}

	private boolean isMap(String messageType) {
		return false;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","varnishMessage"));		
	}


}
