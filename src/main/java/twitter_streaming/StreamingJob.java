/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package twitter_streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

		//Read the twitter keys from config file, environment variable or ...
		Properties twitterCredentials = getTwitterCredentials();
		DataStream<String> tweetStream = env.addSource(new TwitterSource(twitterCredentials));

		tweetStream
				.flatMap(new TweetParser())
				.map(new TweetKeyValue())
				.keyBy(new KeySelector<Tuple2<Tweet, Integer>, String>() {
					@Override
					public String getKey(Tuple2<Tweet, Integer> tweetIntegerTuple2) throws Exception {
						return tweetIntegerTuple2.f0.source;
					}
				})
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.sum(1)
				.print();


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class TweetKeyValue implements MapFunction<Tweet, Tuple2<Tweet, Integer>>{

		/**
		 * The mapping method. Takes an element from the input data set and transforms
		 * it into exactly one element.
		 *
		 * @param tweet The input value.
		 * @return The transformed value
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 *                   to fail and may trigger recovery.
		 */
		@Override
		public Tuple2<Tweet, Integer> map(Tweet tweet) throws Exception {
			return new Tuple2<>(tweet,1);
		}
	}

	public static class TweetParser implements FlatMapFunction<String, Tweet> {

		/**
		 * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
		 * it into zero, one, or more elements.
		 *
		 * @param value The input value.
		 * @param collector   The collector for returning result values.
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 *                   to fail and may trigger recovery.
		 */
		@Override
		public void flatMap(String value, Collector<Tweet> collector) throws Exception {
			Tweet tweet = Tweet.fromString(value);
			if (tweet != null) {
				collector.collect(tweet);
			}
		}
	}

	private static Properties getTwitterCredentials(){
		Properties twitterCredentials = new Properties();

		twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "WfU3vqLwJneAxjwyfAMzidpo2");
		twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "XXTuI8cvNQNCIFLgCj56tRPvlxFGjfAWZ5bkEV7L0u0fNBYivp");
		twitterCredentials.setProperty(TwitterSource.TOKEN, "35245669-LucieaYIvJSgOCJ52rr2PoYTritAeXpssT37IBS8X");
		twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "T0YYlBG8WyF9vpLJFHvwkSsaGjM9KyqxWRDiS4fUPzvaS");
		return  twitterCredentials;
	}
}
