package com.ml.storm.varnishlog.spouts;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Queue;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.ml.storm.varnishlog.dto.VarnishLog;

public class LogsClient implements Runnable{
	HttpClient client;
	Queue<VarnishLog> queue;
	private String host;
	private Integer port;
	private String queueName;
	
	Logger log = Logger.getLogger(LogsClient.class);
	private boolean finished = false;
	
	public LogsClient(Queue<VarnishLog> queue,String host, Integer port,String queueName) {
		this.queue = queue;
		client = new HttpClient();
		this.host = host;
		this.port = port;
		this.queueName = queueName;
	}
	
	@Override
	public void run() {
		HttpGet get;
		while(!finished){
			String url = null;
			try {
				url = getHostUrl(host,port);
				get = new HttpGet(url);
				log.info("Executing get to: "+url);
				DefaultHttpClient client = new DefaultHttpClient();
				HttpResponse response = client.execute(get);
				InputStream stream = response.getEntity().getContent();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
				String line;
				log.info("Start reading");
				while((line = reader.readLine())!=null){
					queue.add(new VarnishLog(host,line));
				}
				log.info("Connection to ["+url+"] lost");
			} catch(HttpHostConnectException e){
				log.warn("Connection error to host ["+url+"]");
			} catch (Exception e) {
				log.error("Error getting messages from ["+url+"]",e);
			}finally{
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {}
			}
		}
		log.info("Thread interrupter, finishing thread");
	}

	private String getHostUrl(String host, Integer port) {
		return "http://"+host+":"+port+"/"+queueName;
	}

	public void setFinished() {
		finished  = true;
	}

}
