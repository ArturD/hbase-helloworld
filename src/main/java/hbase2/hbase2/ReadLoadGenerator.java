package hbase2.hbase2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadLoadGenerator {
	public static void main(String[] args) throws IOException {

		Properties prop = System.getProperties();
		System.out.println(prop.getProperty("java.class.path", null));

		List<String> lines = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				InsertLoadGenerator.class.getResourceAsStream("/pg5000.txt")));
		String line = br.readLine();
		while (line != null) {
			if ("".equals(line.trim())) {
				line = br.readLine();
				continue;
			}
			lines.add(line);
			line = br.readLine();
			System.out.println(line);
		}

		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these
		// can be found on the CLASSPATH
		Configuration config = HBaseConfiguration.create();

		// This instantiates an HTable object that connects you to the
		// "myLittleHBaseTable" table.
		HTable table = new HTable(config, "myLittleHBaseTable");

		Get g;
		Result r;
		String hash;
		long start, finish, diff;

		for (String currline : lines) {
			hash = DigestUtils.shaHex(currline);
			// Now, to retrieve the data we just wrote. The values that come
			// back
			// are Result instances. Generally, a Result is an object that will
			// package up the hbase return into the form you find most
			// palatable.
			g = new Get(Bytes.toBytes("row-" + hash));
			start = Calendar.getInstance().getTimeInMillis();
			r = table.get(g);
			byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"),
					Bytes.toBytes("someQualifier"));
			finish = Calendar.getInstance().getTimeInMillis();
			diff = finish - start;
			System.out.println("Get " + Bytes.toString(g.getRow())
					+ " - took " + diff + " ms");
			// If we convert the value bytes, we should get back 'Some Value',
			// the
			// value we inserted at this location.
			// valueStr = Bytes.toString(value);
			// System.out.println("GET: " + valueStr);
		}
	}
}
