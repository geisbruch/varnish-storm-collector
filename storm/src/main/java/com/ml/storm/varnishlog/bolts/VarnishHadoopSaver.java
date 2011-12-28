package com.ml.storm.varnishlog.bolts;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.ml.storm.varnishlog.dto.VarnishMessage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VarnishHadoopSaver implements IBasicBolt {

	
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH_mm_00");
	String namenodeUrl;
	Path filesPath;
	Configuration conf;
	SnappyCodec codec; 
	FileSystem fs;
	Integer id;
	Long nextCheck;
	Long interval;
	Path actualPathWithoutTmp;
	Path actualPath;
	private OutputStream actualOut;
	
	static Logger LOG = Logger.getLogger(VarnishHadoopSaver.class);
	
	@Override
	public void cleanup() {
		try{
			if(actualOut != null)
				actualOut.close();
			fs.close();
		}catch(Exception e){
			LOG.error("Error cleaning up",e);
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			if(System.currentTimeMillis()>nextCheck){
				if(fs != null){
					fs.close();
				}
				LOG.info("Opening hadoop fs");
				fs = FileSystem.get(conf);
				if(actualOut != null){
					actualOut.close();
					fs.rename(actualPath, actualPathWithoutTmp);
				}
				nextCheck = System.currentTimeMillis()+interval;
				Path newFile = getNewFile();
				actualPathWithoutTmp = newFile;
				actualPath = new Path(newFile+".tmp");
				LOG.info("Creating new file ["+newFile+"]");
				FSDataOutputStream out = fs.create(actualPath);
				LOG.info("Creating output stream");
				actualOut = codec.createOutputStream(out);
//				actualOut = out;
			}
			
			Map<String, Object> map = (Map<String, Object>) input.getValueByField("varnishMessage");
			actualOut.write((JSONObject.toJSONString(map)+"\n").getBytes());
			
		} catch (IOException e) {
			LOG.error("Error writing in hadoop",e);
			LOG.error("Sleeping 5 secconds");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
			}
			nextCheck = 0l;
		}
	}

	private Path getNewFile() {
		String formatTime = format.format(Calendar.getInstance().getTime());
		return new Path(filesPath.toUri()+"/"+formatTime+"."+id+".snappy");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		namenodeUrl = (String) stormConf.get("namenodeUrl");
		filesPath = new Path((String) stormConf.get("hadoopPath"));
		id = context.getThisTaskId(); 
		interval = ((Long)stormConf.get("intervalInMinutes")) * 60 * 1000;
		nextCheck = 0l;
		
		conf = new Configuration();

		conf.set("fs.default.name", namenodeUrl);
		conf.set("dfs.block.size", "134217728");
		conf.set("dfs.blocksize", "134217728");
		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.SnappyCodec");
		
		codec = new SnappyCodec();
		codec.setConf(conf);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}

