import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeSet;

/**
 *
 *    THIS CLASS CAN BE USED TO SEARCH IN YOUR INDEX
 *    FIRST YOU NEED TO INDEX THE COLLECTION.
 *
 */
public class App {

    public static void main(String[] args) throws IOException {
        Scanner scanner= new Scanner(System.in);
        System.out.println("The output of the job (e.g.: hdfs://bigdata01.ad.nith.no/user/taknai/output-indexing):");
        String line;
        while (scanner.hasNext() && ((line=scanner.nextLine()).length()>0)) {
            if (IDX.words.size() > 0) {
                searchIndex(line);
            }else {
                createIndex(line);
            }
            System.out.println("------------------------------------------------\nEnter your search string:");
        }
    }

    private static void searchIndex(String line) {
        line=line.toLowerCase().trim();
        final TreeSet<FileName> fileNames = IDX.words.get(line);
        System.out.println("found "+fileNames.size()+" result(s):");
        for (FileName f : fileNames) {
            System.out.println(f.fileName+":"+f.count);
        }
    }

    private static void createIndex(String line) throws IOException {
        line += "/part-r-00000";
        if (!line.startsWith("/user") && !line.startsWith("hdfs")) {
            line = "/user/" + System.getProperty("user.name") + "/" + line;
        }
        if (!line.startsWith("hdfs://")) {
            line = "hdfs://localhost" + line;
        }
        System.out.println("using " + line);
        final FileSystem fs = FileSystem.get(URI.create(line), new Configuration());
        final FSDataInputStream is = fs.open(new Path(line));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String l;
        while ((l = br.readLine()) != null) {
            IDX.add(l);
        }
        System.out.println("------------------------------------------------\ndone reading, dictionary size: "+IDX.words.size());
        IOUtils.closeStream(is);
    }


    private static class FileName implements Comparable<FileName>{
        String fileName;
        int count;
        private FileName(String fileName, int count) {
            this.fileName = fileName;
            this.count = count;
        }
        @Override
        public int compareTo(FileName o) {
            return (count > o.count) ? -1 : ((count == o.count) ? 0 : 1);
        }
    }

    private static class IDX{
        static final Map<String, TreeSet<FileName>> words=new HashMap<String, TreeSet<FileName>>();

        public static void add(String line) {
            try {
                final String[] split = line.split("\t");
                TreeSet<FileName> treeSet = words.get(split[0]);
                if(treeSet==null)words.put(split[0],treeSet=new TreeSet<FileName>());
                treeSet.add(new FileName(split[1],Integer.parseInt(split[2])));
            } catch (Exception e) {
                System.err.println("error processing "+line);
            }
        }
    }
}
