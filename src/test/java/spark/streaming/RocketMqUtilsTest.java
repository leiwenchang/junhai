/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.streaming;

import com.deal;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spark.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class RocketMqUtilsTest implements Serializable {


    private static String NAME_SERVER = "192.168.137.102:9876";

    private static String TOPIC_DEFAULT = "hello";

    private static int MESSAGE_NUM = 100;

    @Test
    public void testConsumer() throws MQBrokerException, MQClientException, InterruptedException, UnsupportedEncodingException {

        // start up spark
        Map<String, String> optionParams = new HashMap<String, String>();
        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, NAME_SERVER);
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(1000));
        List<String> topics = new ArrayList<String>();
        topics.add(TOPIC_DEFAULT);

        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();

        JavaInputDStream<MessageExt> stream = RocketMqUtils.createJavaMQPullStream(sc, UUID.randomUUID().toString(),
                topics, ConsumerStrategy.earliest(), false, false, false, locationStrategy, optionParams);


        final Set<MessageExt> result = Collections.synchronizedSet(new HashSet<MessageExt>());

        stream.foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
                deal.process(messageExtJavaRDD.rdd());
                result.addAll(messageExtJavaRDD.collect());
            }
        });

        sc.start();

        long startTime = System.currentTimeMillis();
        boolean matches = false;
        while (!matches && System.currentTimeMillis() - startTime < 20000) {
            matches = MESSAGE_NUM == result.size();
            Thread.sleep(50);
        }

        System.out.println("hello");
        sc.stop();
    }


}
