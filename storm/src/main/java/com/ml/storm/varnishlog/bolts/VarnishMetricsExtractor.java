package com.ml.storm.varnishlog.bolts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.esotericsoftware.minlog.Log;
import com.newrelic.api.agent.NewRelic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TimeCacheMap;

public class VarnishMetricsExtractor implements IBasicBolt {
	private static final String NULL_WORD = "NULL";
	public static String ENTITY_POOL="pool";
	public static String ENTITY_DOMAIN="domain";
	public static String ENTITY_HOST="host";
	

	
	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, Object> obj = (Map<String, Object>) input.getValueByField("varnishMessage");
		Map<String, Object> headers = null;
		String host = (String)obj.get("host");;
		String pool = (String)obj.get("pool");
		String domain = null;
		
		if(obj.containsKey("clientRxHeaders")){
			headers = (Map<String, Object>) obj.get("clientRxHeaders");
		}
		if(headers != null){
			domain = (String) headers.get("host");
		}
		
		
		if(!obj.containsKey("clientFetchError")){
			if(obj.containsKey("hasBackend") && new Boolean(obj.get("hasBackend").toString())){
				sendMetric(host,domain,pool,collector,"cacheResponse","miss",1);
			}else{
				sendMetric(host,domain,pool,collector,"cacheResponse","hit",1);
			}
		}else{
			sendMetric(host,domain,pool,collector,"errors",obj.get("clientFetchError").toString(),1);
		}
		
		if(obj.containsKey("clientTxStatus"))
			sendMetric(host,domain,pool,collector,"status",obj.get("clientTxStatus").toString(),1);
		
		if(obj.containsKey("clientReqEnd")){
			String[] reqs = obj.get("clientReqEnd").toString().split(" ");
			if(reqs.length == 6){
				Float start = new Float(reqs[1])*1000;
				Float end = new Float(reqs[2])*1000;
				Long respTimeLong = end.longValue() - start.longValue();
				Float backendTime = new Float(reqs[4])*1000;
				sendMetric(host,domain,pool,collector,"responseTime","total",respTimeLong);
				sendMetric(host,domain,pool,collector,"responseTime","backend",backendTime.longValue());
			}
		}
	}

	private void sendMetric(String host, String domain, String pool,
			BasicOutputCollector collector, String metricGroup, String metricName, Number counter) {
		if(counter == null)
			return;
		
		metricGroup = metricGroup == null ? NULL_WORD:metricGroup;
		metricName = metricName == null ? NULL_WORD:metricName;
		
		if(host!=null)
			collector.emit(new Values(ENTITY_HOST,host,metricGroup,metricName,counter));
		if(domain!=null)
			collector.emit(new Values(ENTITY_DOMAIN,domain,metricGroup,metricName,counter));
		if(pool!=null)
			collector.emit(new Values(ENTITY_POOL,pool,metricGroup,metricName,counter));
	}


	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		/*
		 * Example:
		 * 	/domain/www.mercadolibre.com.ar/responseTime/backend/1
		 *  /host/hdslhp08/responseStatus/200/1
		 *  /pool/arquitectura-img-varnish/cacheResponse/hit/1
		 */
		declarer.declare(new Fields("groupEntityName","groupEntity","metricGroup","metricName","value"));
	}

}

