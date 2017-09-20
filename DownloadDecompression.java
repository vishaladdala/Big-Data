package BDMA1.BDMA1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class DownloadDecompression {

	public static void main(String[] args) throws Exception {

		String homePath = "hdfs://cshadoop1/user/pxv162030/assignment1/part1/";
		String[] books = {"20417.txt.bz2", "5000-8.txt.bz2", "132.txt.bz2", "1661-8.txt.bz2", "972.txt.bz2", "19699.txt.bz2"};
		String url = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/";
		ReadableByteChannel readableBC = null;
		FileOutputStream fileOS = null;
		for (String book : books) {
			try {
				URL urlObj = new URL(url + book);
				readableBC = Channels.newChannel(urlObj.openStream());
				fileOS = new FileOutputStream(book);
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
		}

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		for (String book : books) {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(book));
			URI uri = URI.create(book);
			FileSystem fs = FileSystem.get(uri, conf);
			OutputStream out = fs.create(new Path(homePath + book), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});
			System.out.println();
            IOUtils.copyBytes(inputStream, out, 4096, true);
        }

		for (String book: books) {
			File file = new File(book);
			file.delete();
		}

		for (String book: books) {
			String uri = homePath + book;
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path inputPath = new Path(uri);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(inputPath);
			if (codec == null) {
				System.err.println("No codec found for " + uri);
				System.exit(1);
			}

			String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

			InputStream in = null;
			OutputStream out = null;
			try {
				in = codec.createInputStream(fs.open(inputPath));
				out = fs.create(new Path(outputUri));
				IOUtils.copyBytes(in, out, conf);
			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
			}
		}

		//Deletion
		for (String book: books) {
			String booksURI = homePath + book;
			FileSystem fs = FileSystem.get(URI.create(booksURI), conf);
			fs.delete(new Path(booksURI), true);
		}

	}
}
