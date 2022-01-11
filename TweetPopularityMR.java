package cmsc433.p5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 3;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;
	
	private static final IntWritable one = new IntWritable(1);
	private static final IntWritable rt = new IntWritable(RETWEET_SCORE);

	// MAPPER CLASS
	public static class TweetMapper
	extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());
			Text t;
			// Parses the relevant info from the tweet, depending on the trendingOn param
			
			if (trendingOn == TrendingParameter.USER) {
				t = new Text(tweet.getUserScreenName());
				context.write(t, one);
				
				if (tweet.wasRetweetOfUser()) {
					t = new Text(tweet.getRetweetedUser());
					context.write(t, rt);
				}
				
				for (String user : tweet.getMentionedUsers()) {
					Text t2 = new Text(user);
					context.write(t2, one);
				}
				
			} else if (trendingOn == TrendingParameter.TWEET) {
				t = new Text(String.valueOf(tweet.getId()));
				context.write(t, one);
				
				if (tweet.wasRetweetOfTweet()) {
					t = new Text(String.valueOf(tweet.getRetweetedTweet()));
					context.write(t, rt);
				}
				
			} else if (trendingOn == TrendingParameter.HASHTAG) {
				
				for (String tag : tweet.getHashtags()) {
					t = new Text(tag);
					context.write(t, one);
				}
				
			} else if (trendingOn == TrendingParameter.HASHTAG_PAIR) {
				ArrayList<String> tags = new ArrayList<String>(tweet.getHashtags());
				HashSet<String> pairs = new HashSet<String>();
				
				for (String tag : tags) {
					tag.replaceAll("#", "");
				}
				// Format the hashtag pairs as strings
				for (String first: tags) {
					for (String second: tags) {

						String s = "";
						
						if (first.compareTo(second) < 0) {
							s += "(" + second + "," + first + ")";
						} else if (first.compareTo(second) > 0) {
							s += "(" + first + "," + second + ")";
						} else {
							continue;
						}
						pairs.add(s);
					}
				}
				// Write EACH PAIR STRING to the context
				for (String pair : pairs) {
					t = new Text(pair);
					context.write(t, one);
				}
				
			}
			
		}
	}

	// REDUCER CLASS
	public static class PopularityReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// Sum up the counts for each key
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));

		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		job.setJobName("Popularity M/R");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);


		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
