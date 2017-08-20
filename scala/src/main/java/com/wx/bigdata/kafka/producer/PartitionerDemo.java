package com.wx.bigdata.kafka.producer;

import kafka.producer.DefaultPartitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by Administrator on 2017/6/24.
 */
public class PartitionerDemo extends DefaultPartitioner {

    public PartitionerDemo(VerifiableProperties props) {
        super(props);
    }

    public int partition(Object key, int numPartitions) {
        String str = (String) key;
        str = str.replaceAll("key_","");
        int num = Integer.valueOf(str);
        if (numPartitions>=2){
            return num % 2;
        } else {
            return 0;
        }
    }
}
