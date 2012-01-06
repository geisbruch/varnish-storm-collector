package com.ml.storm.varnishlog.bolts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;

import com.esotericsoftware.minlog.Log;
import com.ml.storm.varnishlog.dto.VarnishMessage;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TimeCacheMap;

public class VarnishMessageCreator implements IBasicBolt {
	
	private static final String NULL_WORD = "NULL";
	private TimeCacheMap<String,String> hostPool;
	private String apiUrl;
	private Number expirationPoolsCache;
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String dbConnection;
	
	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
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
		
		if(messageList.containsKey("backendLogs"))
			addEntries((List)messageList.get("backendLogs"),obj,"backend");
		
		if(messageList.containsKey("clientLogs"))
			addEntries((List) messageList.get("clientLogs"),obj,"client");
		
		
		obj.put("date", format.format(Calendar.getInstance().getTime()));
		String host = (String) messageList.get("host");
		obj.put("pool", getPool(host));
		collector.emit(new Values(obj));
	}

	private String getPool(String host) {
		if(hostPool.containsKey(host)){
			return hostPool.get(host);
		}
		/*
		 * Check new world 
		 */
		String pool = getPoolFromNewWorld(host);
		if(pool != null){
			hostPool.put(host, pool);
			return pool;
		}
		
		pool = getPoolFromOldWorld(host);
		
		if(pool != null){
			hostPool.put(host, pool);
			return pool;
		}
		
		hostPool.put(host, NULL_WORD);
		
		return NULL_WORD;
	}

	/**
	 * Try to get pool from the mysql database (old world)
	 * @param host
	 * @return
	 */
	private String getPoolFromOldWorld(String host) {
		Connection connection;
		try {
			connection = DriverManager.getConnection(dbConnection);
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("select pool_name from server_description where server_name='"+host+"'");
			if(rs.next()){
				return rs.getString("pool_name");
			}
		} catch (SQLException e) {
			Log.error("Error getting data from db",e);
		}
		return null;
	}

	/**
	 * Try to get the pool name from de infraestructure api
	 * 
	 * @param host
	 * @return
	 */
	private String getPoolFromNewWorld(String host) {
		try{
			DefaultHttpClient client = new DefaultHttpClient();
			HttpGet get = new HttpGet(apiUrl+"/servers/"+host);
			HttpResponse response = client.execute(get);
			if(response.getStatusLine().getStatusCode() != 200)
				return null;
			
			String in;
			StringBuffer resp = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			while((in = reader.readLine()) != null){
				resp.append(in);
				resp.append("\n");
			}
			JSONObject json = (JSONObject)JSONUtils.parseJson(resp.toString());
			if(json.containsKey("pool"))
				return json.get("pool").toString();
			else return null;
		}catch(Exception e){
			Log.error("Error comunicating with new world api",e);
		}
		return null;
	}
		
	/**
	 * Create the entries to emit
	 * @param messageList
	 * @param obj
	 * @param prefix
	 */
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

	/**
	 * Parse and add headers
	 * 
	 * @param messageLog
	 * @param map
	 */
	private void addHeader(String messageLog, Map<String, Object> map) {
		int idx = messageLog.indexOf(":");
		if(idx!=-1){
			String headerName = messageLog.substring(0,idx);
			String headerContent = "";
			if(messageLog.length() >= idx){
				headerContent = messageLog.substring(idx+1);
			}
			map.put(headerName.toLowerCase(), headerContent.toLowerCase());
		}
	}

	/**
	 * Create a simple object name
	 * @param connectionPrefix
	 * @param messageType
	 * @return
	 */
	private String getObjByCon(String connectionPrefix, String messageType) {
		return connectionPrefix+messageType;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error loading mysql driver",e);
		}
		this.dbConnection = stormConf.get("dbConnection").toString();
		this.apiUrl = stormConf.get("infraApiUrl").toString();
		this.expirationPoolsCache = (Number)stormConf.get("expirationPoolsCache");
		hostPool = new TimeCacheMap<String, String>(expirationPoolsCache.intValue());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("varnishMessage"));		
	}


}
