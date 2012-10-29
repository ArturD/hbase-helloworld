package hbase2.hbase2;

import java.io.IOException;
import java.util.Calendar;
import java.util.Properties;

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
		String valueStr;
		long start, finish, diff;

		for (int i = 0; i < 1000000; i++) {

			// Now, to retrieve the data we just wrote. The values that come
			// back
			// are Result instances. Generally, a Result is an object that will
			// package up the hbase return into the form you find most
			// palatable.
			g = new Get(Bytes.toBytes("row-" + i));
			start = Calendar.getInstance().getTimeInMillis();
			r = table.get(g);
			byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"),
					Bytes.toBytes("someQualifier"));
			finish = Calendar.getInstance().getTimeInMillis();
			diff = finish - start;
			System.out.println("Get " + Bytes.toString(g.getRow()) + " - " + i
					+ " took " + diff + " ms");
			// If we convert the value bytes, we should get back 'Some Value',
			// the
			// value we inserted at this location.
			// valueStr = Bytes.toString(value);
			// System.out.println("GET: " + valueStr);
		}
	}
}
