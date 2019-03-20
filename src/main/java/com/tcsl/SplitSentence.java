package com.tcsl;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 将句子进行拆分
 */
public class SplitSentence implements IRichBolt {

    private OutputCollector collector;

    public SplitSentence() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 每接收到一条数据，就会让execute来执行。
     *
     * @param tuple
     */
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("word");
        String[] split = sentence.split(" ");
        for (String s : split) {
            collector.emit(new Values(s));
        }
    }

    public void cleanup() {

    }
}