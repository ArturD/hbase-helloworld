package hbase2.hbase2;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
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

		// all files in this directory will be stored in HBase
		// on per line basics. It's key is hash of the line content.
		// You can use text ebooks from http://www.gutenberg.org/ 
		String textFilesPath = "/home/rszalla/git/hbase-helloworld/teksty";

		String file;
		File folder = new File(textFilesPath);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				file = listOfFiles[i].getAbsolutePath();
				System.out.println("Processing file " + file);

				FileInputStream fstream = new FileInputStream(file);
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));

				List<String> lines = new ArrayList<String>();

				String line = br.readLine();
				while (line != null) {
					if ("".equals(line.trim())) {
						line = br.readLine();
						continue;
					}
					lines.add(line);
					line = br.readLine();
//					System.out.println(line);
				}

				Configuration config = HBaseConfiguration.create();

				HTable table = new HTable(config, "myLittleHBaseTable");
				long start, finish, diff;
				String hash;

				for (String currline : lines) {
					hash = DigestUtils.shaHex(currline);

					Put p = new Put(Bytes.toBytes("row-" + hash));

					p.add(Bytes.toBytes("myLittleFamily"),
							Bytes.toBytes("someQualifier"),
							Bytes.toBytes(currline));

					start = Calendar.getInstance().getTimeInMillis();
					table.put(p);
					finish = Calendar.getInstance().getTimeInMillis();
					diff = finish - start;
					System.out.println("Insert " + hash + " took " + diff
							+ " ms");
				}

			}
		}

	}
}
