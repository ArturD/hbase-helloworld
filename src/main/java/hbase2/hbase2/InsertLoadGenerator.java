package hbase2.hbase2;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class InsertLoadGenerator {
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
		long start, finish, diff;
		String hash;

		for (String currline : lines) {
			hash = DigestUtils.shaHex(currline);

			// To add to a row, use Put. A Put constructor takes the name of the
			// row
			// you want to insert into as a byte array. In HBase, the Bytes
			// class
			// has utility for converting all kinds of java types to byte
			// arrays. In
			// the below, we are converting the String "myLittleRow" into a byte
			// array to use as a row key for our update. Once you have a Put
			// instance, you can adorn it by setting the names of columns you
			// want
			// to update on the row, the timestamp to use in your update, etc.
			// If no timestamp, the server applies current time to the edits.
			Put p = new Put(Bytes.toBytes("row-" + hash));

			// To set the value you'd like to update in the row 'myLittleRow',
			// specify the column family, column qualifier, and value of the
			// table
			// cell you'd like to update. The column family must already exist
			// in your table schema. The qualifier can be anything.
			// All must be specified as byte arrays as hbase is all about byte
			// arrays. Lets pretend the table 'myLittleHBaseTable' was created
			// with a family 'myLittleFamily'.
			p.add(Bytes.toBytes("myLittleFamily"),
					Bytes.toBytes("someQualifier"), Bytes.toBytes(currline));

			// Once you've adorned your Put instance with all the updates you
			// want
			// to make, to commit it do the following
			// (The HTable#put method takes the Put instance you've been
			// building
			// and pushes the changes you made into hbase)
			start = Calendar.getInstance().getTimeInMillis();
			table.put(p);
			finish = Calendar.getInstance().getTimeInMillis();
			diff = finish - start;
			System.out.println("Insert " + hash + " took " + diff + " ms");
		}

	}
}
