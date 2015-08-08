package trial.clustering;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import us.jubat.clustering.ClusteringClient;
import us.jubat.clustering.WeightedDatum;
import us.jubat.common.Datum;
import us.jubat.common.Datum.StringValue;



public class JubaClustTweet {
	//	private final static String TwitterPath = "/Users/hiroki/Desktop/MIJS_Data/twitter_mecab.csv";
	//	private final static String TwitterPath = "/Users/hiroki/Desktop/MIJS_Data/twitter_mecab_10000.csv";
		private final static String TwitterPath = "/Users/hiroki/Desktop/MIJS_Data/twitter_5000_space.csv";
//	private final static String TwitterPath = "/Users/hiroki/Desktop/MIJS_Data/twitter_1000_space.csv";

	private final ClusteringClient client;
	private final Random random;

	public JubaClustTweet(ClusteringClient client) {
		this.client = client;
		this.random = new Random(0);
	}

	public static void main(String[] args) {
		//String host = "127.0.0.1";
		String host = "192.168.56.101";
		try {
			ClusteringClient client = new ClusteringClient(host, 9199, "test", 1800);
			JubaClustTweet s = new JubaClustTweet(client);
			// TODO push data
			s.client.clear();
			s.tweetReadAndPush(TwitterPath);
			List<Datum> kList = client.getKCenter();
			System.out.println("=== k size : " + kList.size());
			//			for (Datum kDatum : kList) {
			//				StringValue strVal = kDatum.stringValues.get(0);
			//				System.out.println(strVal.key + " " + strVal.value);
			//			}

			List<List<WeightedDatum>> coreList = client.getCoreMembers();
			System.out.println("=== core size : " + coreList.size());
			int idx = 1;
			for (List<WeightedDatum> wDList : coreList) {
				System.out.println("=== CoreList " + idx + " size " + wDList.size());
				for (WeightedDatum wDatum : wDList) {
					for (StringValue val : wDatum.point.stringValues) {
						System.out.println("--- " + val.key + " " + val.value);
					}
				}
				idx++;
			}

			// TODO 検証
			Datum data = makeDatum("tweet", "通院 途切れ 今日"); // 12件：モーニング

			System.out.println("=== provide");
			outDatumData(data);
			System.out.println("=== nearest menbers");
			List<WeightedDatum> members = client.getNearestMembers(data);
			System.out.println(members.size());
			//			System.out.println("=== nearest menbers datum");
			//			for (WeightedDatum wDatum : members) {
			//				StringValue val = wDatum.point.stringValues.get(0);
			//				System.out.println(wDatum.weight + ":" + val.key + " " + val.value);
			//				NumValue val = wDatum.point.numValues.get(0);
			//				System.out.println(wDatum.weight + ":" + val.key + " " + val.value);
			//			}

			//			System.out.println("=== provide");
			//			outDatumData(data);
			//			Datum nearDatum = client.getNearestCenter(data);
			//			System.out.println("=== nearest center");
			//			outDatumData(nearDatum);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	public void tweetReadAndPush(String filePath) {
		try (FileInputStream fis = new FileInputStream(filePath);
				InputStreamReader in = new InputStreamReader(fis,"UTF-8");
				BufferedReader inFile = new BufferedReader(in);
				) {
			int pushCnt = 0;
			String line;
			String tweet;
			List<Datum> datums = new ArrayList<Datum>();
			while ((line = inFile.readLine()) != null) {
				// Tweet内容取得
				tweet = getTweet(line);
				if (tweet.contains("RT @")) continue;
				// TODO Tweetデータ格納
				//				datums.add(makeDatum(String.valueOf(random.nextInt(1000)), tweet));
				datums.add(makeDatum("tweet", tweet));
				pushCnt++;
				if (0 == (pushCnt % 250)) {
					client.push(datums);
					System.out.println("num of push : " + pushCnt + " : " + tweet);
					datums.clear();
				}
			}
			if (!datums.isEmpty()) {
				System.out.println("datum is not empty");
				client.push(datums);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 行内Tweetデータ取得
	 * @param lineData
	 * @return
	 */
	private String getTweet(String lineData) {
		int first = lineData.indexOf(",");
		lineData = lineData.substring(first+1);
		int secound = lineData.indexOf(",");
		return lineData.substring(secound+1);
	}
	/**
	 * Datum構築
	 * @param x
	 * @param y
	 * @return
	 */
	private static Datum makeDatum(String x, String y) {
		Datum datum = new Datum();
		datum.addString(x, y);
		return datum;
	}


	//	private Datum getTweetCell(String tweet) {
	//		String[] words = tweet.split("¥t");
	//		HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
	//    	// すでにIDが含まれる
	//    	if (wordMap.containsKey(words)) {
	//    		wordMap.put(tmp, wordMap.get(tmp) + 1);
	//    	} else {
	//    		idMap.put(tmp, 1);
	//    	}
	//
	//	}

	private static void outDatumData(Datum data) {
		System.out.println(
				data.stringValues.get(0).key + ":" + 
						data.stringValues.get(0).value);
	}
}
