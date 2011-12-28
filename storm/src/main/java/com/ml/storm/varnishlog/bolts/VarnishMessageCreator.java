package com.ml.storm.varnishlog.bolts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		Map<Object,Object> messageList = (Map) input.getValueByField("message");
		Map<String, Object> obj = new HashMap<String, Object>();
		
		/**
		 * For each message check convert type and add to a map
		 */
		for(Map.Entry<Object,Object> message : messageList.entrySet()){

			if(!"backendLogs".equals(message.getKey().toString()) && !"clientLogs".equals(message.getKey().toString())){
				obj.put(message.getKey().toString(), message.getValue());
			}
			
		}
		
		addEntries((List)messageList.get("backendLogs"),obj,"backend");
		addEntries((List) messageList.get("clientLogs"),obj,"client");
		
		System.out.println(JSONObject.toJSONString(obj));
		
		collector.emit(new Values(obj));
	}

		
	private void addEntries(List messageList, Map<String, Object> obj, String prefix) {
		for(Object message : messageList){
			String[] split = message.toString().split("\t");
			if(split.length == 2){
				String messageType = split[0];
				String messageLog = split[1];
				String objName = getObjByCon(prefix,messageType);
				
				if(messageType.toLowerCase().contains("header")){
					objName = objName+"s";
					/**
					 * if the message it's a header we will create an other map
					 */
					if(!obj.containsKey(objName)){
						obj.put(objName, new HashMap<String, Object>());
					}
					Map<String, Object> map = (Map<String, Object>)obj.get(objName);
					addHeader(messageLog,map);
				}else if(messageType.startsWith("VCL_")){
					/**
					 * If it's a vcl we will use a list 
					 */
					objName = getObjByCon(prefix,"VCL");
					if(!obj.containsKey(objName)){
						obj.put(objName, new ArrayList<String>());
					}
					((List<String>)obj.get(objName)).add(messageType.split("_")[1]+" "+messageLog);
				}else{
					/**
					 * Else assume string
					 */
					obj.put(objName, messageLog);
				}
			}
		}
	}


	private String getDate() {
		return format.format(Calendar.getInstance().getTime());
	}

	private void addHeader(String messageLog, Map<String, Object> map) {
		int idx = messageLog.indexOf(":");
		if(idx!=-1){
			String headerName = messageLog.substring(0,idx);
			String headerContent = "";
			if(messageLog.length() >= idx){
				headerContent = messageLog.substring(idx+1);
			}
			map.put(headerName, headerContent);
		}
	}

	private String getObjByCon(String connectionPrefix, String messageType) {
		return connectionPrefix+messageType;
	}

	private boolean isMap(String messageType) {
		return false;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("varnishMessage"));		
	}


}
