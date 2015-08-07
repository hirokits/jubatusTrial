package trial.clustering;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
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
	private final static String TwitterPath = "/Users/hiroki/Desktop/MIJS_Data/twitter_mecab_10000.csv";

	private final ClusteringClient client;
	private final Random random;

	public JubaClustTweet(ClusteringClient client) {
		this.client = client;
		this.random = new Random(0);
		// clear datum on cluster
//		client.clear();
	}

	public void tweetReadAndPush(String filePath) {
		FileInputStream fis = null; 
		InputStreamReader in = null; 
		BufferedReader inFile = null;
		try {
			fis = new FileInputStream(filePath); 
			in = new InputStreamReader(fis,"UTF-8"); 
			inFile = new BufferedReader(in);

			int pushCnt = 0;
			String line;
			String tweet;
			List<Datum> datums = new ArrayList<Datum>();
			while ((line = inFile.readLine()) != null) {
				// Tweet内容取得
				tweet = getTweet(line);
				// TODO Tweetデータ格納
//				datums.add(makeDatum("tweet" + random.nextInt(10), tweet));
				datums.add(makeDatum(String.valueOf(random.nextInt(1000)), tweet));
				//datums.add(makeDatum("tweet", tweet));
				pushCnt++;
				if (0 == (pushCnt % 500)) {
					client.push(datums);
					System.out.println("num of push : " + pushCnt + " : " + tweet);
					datums.clear();
					Thread.sleep(2000L);
				}
			}
			if (!datums.isEmpty()) {
				System.out.println("datum is not empty");
				client.push(datums);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				inFile.close();
				in.close();
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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

	public static void main(String[] args) {
		//String host = "127.0.0.1";
		String host = "192.168.56.101";
		try {
			ClusteringClient client = new ClusteringClient(host, 9199, "test", 180);
			JubaClustTweet s = new JubaClustTweet(client);
			// TODO push data
//			s.tweetReadAndPush(TwitterPath);
			List<Datum> kList = client.getKCenter();
			System.out.println("=== k size : " + kList.size());
//			for (Datum kDatum : kList) {
//				StringValue strVal = kDatum.stringValues.get(0);
//				System.out.println(strVal.key + " " + strVal.value);
//			}
			
			List<List<WeightedDatum>> coreList = client.getCoreMembers();
			System.out.println("=== core size : " + coreList.size());
//			for (List<WeightedDatum> wDList : coreList) {
//				System.out.println(wDList.size());
//				for (WeightedDatum wDatum : wDList) {
//					StringValue strVal = wDatum.point.stringValues.get(0);
//					System.out.println(wDatum.weight + ":" + strVal.key + " " + strVal.value);
//				}
//			}
			
			// TODO 検証
//			Datum data = makeDatum("tweet", "伊豆高原	駅	つい	ねよ"); // 4件：伊豆高原
			Datum data = makeDatum("tweet", "モーニング"); // 12件：モーニング

			System.out.println("=== provide");
			outDatumData(data);
			System.out.println("=== nearest menbers");
			List<WeightedDatum> members = client.getNearestMembers(data);
			System.out.println(members.size());
			System.out.println("=== nearest menbers datum");
			for (WeightedDatum wDatum : members) {
				StringValue val = wDatum.point.stringValues.get(0);
//				NumValue val = wDatum.point.numValues.get(0);
				System.out.println(wDatum.weight + ":" + val.key + " " + val.value);
			}

			System.out.println("=== provide");
			outDatumData(data);
			Datum nearDatum = client.getNearestCenter(data);
			System.out.println("=== nearest center");
			outDatumData(nearDatum);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	private static void outDatumData(Datum data) {
		System.out.println(
				data.stringValues.get(0).key + ":" + 
						data.stringValues.get(0).value);
	}
}
