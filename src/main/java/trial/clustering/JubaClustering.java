package trial.clustering;

import static org.hamcrest.CoreMatchers.nullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.sun.security.ntlm.Client;

import us.jubat.clustering.ClusteringClient;
import us.jubat.clustering.WeightedDatum;
import us.jubat.common.Datum;
import us.jubat.common.Datum.NumValue;
import us.jubat.common.Datum.StringValue;

public class JubaClustering {
	private final ClusteringClient client;
	private final Random random;

	public JubaClustering(ClusteringClient client) {
		this.client = client;
		this.random = new Random(0);
		// TODO clear datum on cluster
		this.client.clear();
	}

	private void makeAndPush() {
		List<Datum> data_list = new ArrayList<Datum>();
		for (int i = 0; i < 100; i++) {
			data_list.add(generateDatum());
		}
		client.push(data_list);
	}

	public static void main(String[] args) {
		//String host = "127.0.0.1";
		String host = "192.168.56.101";
		try {

			ClusteringClient client = new ClusteringClient(host, 9199, "test", 1);
			JubaClustering s = new JubaClustering(client);
			s.makeAndPush();

			List<Datum> kList = client.getKCenter();
			System.out.println("=== k size : " + kList.size());
			for (Datum kDatum : kList) {
				StringValue strVal = kDatum.stringValues.get(0);
				System.out.println(strVal.key + " " + strVal.value);
			}
			
			List<List<WeightedDatum>> coreList = client.getCoreMembers();
			System.out.println("=== core size : " + coreList.size());
			int idx = 1;
			for (List<WeightedDatum> wDList : coreList) {
				System.out.println("=== CoreList " + idx + " size " + wDList.size());
				for (WeightedDatum wDatum : wDList) {
					StringValue strVal = wDatum.point.stringValues.get(0);
					System.out.println(wDatum.weight + ":" + strVal.key + " " + strVal.value);
				}
				++idx;
			}
			// 検証
			Datum data = generateDatumForTest();
			System.out.println("=== provide");
			outDatumData(data);
			System.out.println("=== nearest menbers");
			List<WeightedDatum> members = client.getNearestMembers(data);
			System.out.println(members.size());
			System.out.println("=== nearest menbers datum");
			for (WeightedDatum wDatum : members) {
//				StringValue strVal = wDatum.point.stringValues.get(0);
				NumValue val = wDatum.point.numValues.get(0);
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
	
	private Datum generateDatum() {
		Datum datum = new Datum();
		for (int i = 1; i < 50; i++) {
			int intId = this.random.nextInt(100);
			datum.addString("key/str" + intId, "val/str" + intId);
		}
		for (int i = 1; i < 50; i++) {
			int intId = this.random.nextInt(100);
			datum.addNumber("key/num" + intId, intId);
		}
		return datum;
	}
	
	private static Datum generateDatumForTest() {
		Datum datum = new Datum();
		datum.addString("key/str50", "val/str50");
		return datum;
	}
}
