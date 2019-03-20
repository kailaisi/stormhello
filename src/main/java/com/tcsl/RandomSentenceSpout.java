package com.tcsl;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * 主要负责从数据源获取数据，本地我们就不获取数据，只是不断地发射数据
 * 数据源的一个代码组件，就是我们可以实现一个spout接口，写一个java类，在这个spout代码中，我们可以自己尝试去数据源获取数据，比如说从kafka中消费数据
 */
public class RandomSentenceSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    /**
     * spout是运行在Worker进程中的Executor中的某个Task中。Task会负责不断的无限轮训调用该方法
     * 只要需要，就可以不断的将数据发射出去，形成数据流
     */
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[]{sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
                sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")};
        final String sentence = sentences[_rand.nextInt(sentences.length)];
        System.err.println("发射数据:"+sentence);
        _collector.emit(new Values(sentence));
    }

    protected String sentence(String input) {
        return input;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    /**
     * 每个发射出去的每个Tuple的每个Field的名称
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
