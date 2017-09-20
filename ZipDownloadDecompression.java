package BDMA1.BDMA1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipDownloadDecompression {

	public static void main(String[] args) throws Exception {

		String homePath = "hdfs://cshadoop1/user/pxv162030/assignment1/part2/";
		String url = "https://corpus.byu.edu/glowbetext/samples/text.zip";

		ReadableByteChannel readableBC = null;
		FileOutputStream fileOS = null;
		try {
			URL urlObj = new URL(url);
			readableBC = Channels.newChannel(urlObj.openStream());
			fileOS = new FileOutputStream("text.zip");
			fileOS.getChannel().transferFrom(readableBC, 0, Long.MAX_VALUE);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileOS != null)
					fileOS.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if (readableBC != null)
					readableBC.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}


		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		InputStream inputStream = new BufferedInputStream(new FileInputStream("text.zip"));
		URI uri = URI.create("text.zip");
		FileSystem fs = FileSystem.get(uri, conf);
		OutputStream out = fs.create(new Path(homePath + "text.zip"), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		System.out.println();
		IOUtils.copyBytes(inputStream, out, 4096, true);

		File file = new File("text.zip");
		file.delete();

		String uri1 = homePath + "text.zip";
		fs = FileSystem.get(URI.create(uri1), conf);
		Path inputPath = new Path(uri1);
		ZipInputStream stream = new ZipInputStream(fs.open(inputPath));
		ZipEntry entry;
		byte[] buffer = new byte[2048];
		while((entry = stream.getNextEntry()) != null) {
			String outputUri = homePath + "text/" + entry.getName();
			out = fs.create(new Path(outputUri));
			int len;
			while ((len = stream.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}
		}

		//Deletion
		String booksURI = homePath + "text.zip";
		fs = FileSystem.get(URI.create(booksURI), conf);
		fs.delete(new Path(booksURI), true);
	}

}
