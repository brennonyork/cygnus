from string import Template

# Outlines the template for all .java ingest programs
TEMPLATE = Template(
"\
$PACKAGE_NAME\
\n\
/*\n\
$CONF_FILE\
*/\n\
$USER_IMPORTS\
\n\
import java.io.DataInputStream;\n\
import java.io.FileNotFoundException;\n\
import java.io.IOException;\n\
import java.io.InputStream;\n\
import java.io.FileInputStream;\n\
import java.io.File;\n\
import java.util.Map.Entry;\n\
import java.util.TreeMap;\n\
import java.util.UUID;\n\
import java.util.concurrent.Semaphore;\n\
import java.util.ArrayList;\n\
\n\
import org.apache.hadoop.conf.Configuration;\n\
import org.apache.hadoop.fs.FSDataInputStream;\n\
import org.apache.hadoop.fs.FSInputStream;\n\
import org.apache.hadoop.fs.FileStatus;\n\
import org.apache.hadoop.fs.FileSystem;\n\
import org.apache.hadoop.fs.Path;\n\
import org.apache.hadoop.io.Text;\n\
\n\
import org.apache.accumulo.core.conf.AccumuloConfiguration;\n\
import org.apache.accumulo.core.data.Key;\n\
import org.apache.accumulo.core.data.Value;\n\
import org.apache.accumulo.core.file.FileOperations;\n\
import org.apache.accumulo.core.file.FileSKVWriter;\n\
import org.apache.accumulo.core.client.Connector;\n\
import org.apache.accumulo.core.client.ZooKeeperInstance;\n\
import org.apache.accumulo.core.client.AccumuloException;\n\
import org.apache.accumulo.core.client.AccumuloSecurityException;\n\
import org.apache.accumulo.core.security.ColumnVisibility;\n\
\n\
public class $CLASS_NAME {\n\
    Configuration conf = new Configuration();\n\
    FileSystem fs = null;\n\
    InputStream[] dis = null;\n\
    String input_dir = \"$INPUT_DIR\";\n\
    String output_dir = \"$OUTPUT_DIR\";\n\
    Boolean hdfs_output = $HDFS_OUTPUT_Q;\n\
    Boolean hdfs_input = $HDFS_INPUT_Q;\n\
    long[] num_recs = null;\n\
    int file_count = 0;\n\
    long[] f_len;\n\
    long[] f_offset;\n\
    static Boolean stream = false;\n\
    Semaphore s = new Semaphore(1);\n\
    Text sb = null;\n\
    $STATIC_TEXT\n\
\n\
    public void disp_help() {\n\
        System.out.println();\n\
        System.out.println(\"$CLASS_NAME Ingestor\");\n\
        System.out.println();\n\
        System.out.println(\"  This is a generated Ingest file from the Cygnus compiler. Below its features\");\n\
        System.out.println(\"  are briefly described. For detailed questions / support email:\");\n\
        System.out.println(\"      brennon.york@gmail.com\");\n\
        System.out.println();\n\
        System.out.println(\"  -lid or --local-input-directory [directory]:\");\n\
        System.out.println(\"    Override the default LOCAL_INPUT_DIR value of the .conf file and read the\");\n\
        System.out.println(\"    directory specified.\");\n\
        System.out.println(\"  -lod or --local-output-directory [directory]:\");\n\
        System.out.println(\"    Override the default LOCAL_OUTPUT_DIR value of the .conf file and read the\");\n\
        System.out.println(\"    directory specified.\");\n\
        System.out.println(\"  -hid or --hdfs-input-directory [directory]:\");\n\
        System.out.println(\"    Override the default HDFS_INPUT_DIR value of the .conf file and read from the\");\n\
        System.out.println(\"    specified HDFS path. This path is not verified and, if specified along with\");\n\
        System.out.println(\"    a < -lid > flag, this will take precedence.\");\n\
        System.out.println(\"  -hod or --hdfs-output-directory [directory]:\");\n\
        System.out.println(\"    Override the default HDFS_OUTPUT_DIR value of the .conf file and read from the\");\n\
        System.out.println(\"    specified HDFS path. This path is not verified and, if specified along with\");\n\
        System.out.println(\"    a < -lod > flag, this will take precedence.\");\n\
        System.out.println(\"  -s or --streaming:\");\n\
        System.out.println(\"    Allows for input to be piped in from stdin. This option still allows for\");\n\
        System.out.println(\"    overriding of output directories, but ignores the THREAD_NUM option.\");\n\
        System.out.println(\"  -dc or --display-conf:\");\n\
        System.out.println(\"    Displays the configuration file used to generate this Ingest file.\");\n\
        System.out.println(\"  -h or --help:\");\n\
        System.out.println(\"    Displays this help menu.\");\n\
        System.out.println();\n\
    }\n\
\n\
    public void disp_conf() {\n\
        $JAVA_CONF_FILE\
    }\n\
\n\
    public static void main(String[] args) {\n\
        long start_time = System.currentTimeMillis();\n\
        $CLASS_NAME i = new $CLASS_NAME(args);\n\
        if(stream) {\n\
            i.startStreaming();\n\
        } else {\n\
            i.start();\n\
        }\n\
        System.out.println((start_time - System.currentTimeMillis()));\n\
    }\n\
\n\
    public $CLASS_NAME(String[] args) {\n\
        for(int i = 0; i < args.length; ++i) {\n\
            String arg = args[i];\n\
            if(arg.equalsIgnoreCase(\"-s\") || arg.equalsIgnoreCase(\"--streaming\")) {\n\
                stream = true;\n\
                continue;\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-lid\") || arg.equalsIgnoreCase(\"--local-input-directory\")) {\n\
                if((i + 1) < args.length && !args[i + 1].isEmpty()) { input_dir = args[++i]; }\n\
                hdfs_input = false;\n\
                continue;\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-lod\") || arg.equalsIgnoreCase(\"--local-output-directory\")) {\n\
                if((i + 1) < args.length && !args[i + 1].isEmpty()) { output_dir = args[++i]; }\n\
                hdfs_output = false;\n\
                continue;\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-hid\") || arg.equalsIgnoreCase(\"--hdfs-input-directory\")) {\n\
                if((i + 1) < args.length && !args[i + 1].isEmpty()) { input_dir = args[++i]; }\n\
                hdfs_input = true;\n\
                continue;\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-hod\") || arg.equalsIgnoreCase(\"--hdfs-output-directory\")) {\n\
                if((i + 1) < args.length && !args[i + 1].isEmpty()) { output_dir = args[++i]; }\n\
                hdfs_output = true;\n\
                continue;\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-h\") || arg.equalsIgnoreCase(\"--help\")) {\n\
                disp_help();\n\
                System.exit(0);\n\
            }\n\
            else if(arg.equalsIgnoreCase(\"-dc\") || arg.equalsIgnoreCase(\"--display-conf\")) {\n\
                disp_conf();\n\
                System.exit(0);\n\
            }\n\
        }\n\
        try {\n\
            fs = FileSystem.get(conf);\n\
        } catch(IOException e) {\n\
            e.printStackTrace();\n\
        }\n\
        this.sb = new Text(new byte[]{0x00,0x00});\n\
    }\n\
\n\
    public void start() {\n\
        if(hdfs_input) {\n\
            Path input_dir = new Path(this.input_dir);\n\
            FileStatus[] files = null;\n\
            try {\n\
                files = fs.listStatus(input_dir);\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            int num_files = files.length;\n\
            this.dis = new InputStream[num_files];\n\
            this.f_len = new long[num_files];\n\
            this.f_offset = new long[num_files];\n\
            for(int i = 0; i < num_files; ++i) {\n\
                try {\n\
                    if(!files[i].isDir()) {\n\
                        this.dis[file_count] = new FSDataInputStream(fs.open(files[i].getPath()));\n\
                        this.f_len[file_count] = files[i].getLen();\n\
                        this.f_offset[file_count] = 0;\n\
                        $HEADER_LEN\n\
                        file_count++;\n\
                    }\n\
                } catch(Exception e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
        } else {\n\
            File input_dir = new File(this.input_dir);\n\
            String[] files = input_dir.list();\n\
            int num_files = files.length;\n\
            this.dis = new InputStream[num_files];\n\
            this.f_len = new long[num_files];\n\
            this.f_offset = new long[num_files];\n\
            for(int i = 0; i < num_files; ++i) {\n\
                try {\n\
                    File f = new File(input_dir.getAbsoluteFile()+File.separator+files[i]);\n\
                    if(f.isFile()) {\n\
                        this.dis[file_count] = new FileInputStream(f);\n\
                        this.f_len[file_count] = f.length();\n\
                        this.f_offset[file_count] = 0;\n\
                        $HEADER_LEN\n\
                        file_count++;\n\
                    }\n\
                } catch(FileNotFoundException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
        }\n\
\n\
        Thread[] workers = new Thread[$NUM_THREADS];\n\
        for(int i = 0; i < $NUM_THREADS; ++i) {\n\
            Worker w = new Worker(this.dis, this.file_count, this.f_len);\n\
            workers[i] = new Thread(w);\n\
            workers[i].start();\n\
        }\n\
\n\
        boolean keepgoing = true;\n\
        try {\n\
            while(keepgoing) {\n\
                Thread.sleep(100);\n\
                keepgoing = false;\n\
                for(int i = 0; i < $NUM_THREADS; ++i) {\n\
                    if(workers[i].isAlive()) {\n\
                        keepgoing = true;\n\
                        break;\n\
                    }\n\
                }\n\
            }\n\
        } catch(InterruptedException e) {\n\
            e.printStackTrace();\n\
        }\n\
\n\
        load();\n\
    }\n\
\n\
    public void startStreaming() {\n\
        if(hdfs_input) {\n\
            System.out.println(\"ERROR: Cannot specify HDFS input when streaming\");\n\
            System.exit(0);\n\
        } else {\n\
            this.dis = new InputStream[1];\n\
            this.f_offset = new long[1];\n\
            this.dis[0] = System.in;\n\
            this.f_offset[0] = 0;\n\
            $HEADER_LEN\n\
        }\n\
\n\
        Thread[] workers = new Thread[1];\n\
        for(int i = 0; i < 1; ++i) {\n\
            StreamingWorker w = new StreamingWorker(this.dis[0]);\n\
            workers[i] = new Thread(w);\n\
            workers[i].start();\n\
        }\n\
\n\
        boolean keepgoing = true;\n\
        try {\n\
            while(keepgoing) {\n\
                Thread.sleep(100);\n\
                keepgoing = false;\n\
                for(int i = 0; i < 1; ++i) {\n\
                    if(workers[i].isAlive()) {\n\
                        keepgoing = true;\n\
                        break;\n\
                    }\n\
                }\n\
            }\n\
        } catch(InterruptedException e) {\n\
            e.printStackTrace();\n\
        }\n\
\n\
        load();\n\
    }\n\
\n\
    public void incShard() {\n\
        byte[] byteShard = this.sb.getBytes();\n\
        int shard = (((((int)byteShard[0]) & 255) <<  8) + (((int)byteShard[1]) & 255));\n\
        shard = ((shard + 1) % $SHARD_NUM);\n\
        this.sb = new Text(new byte[]{(byte)((shard >>> 8) & 255), (byte)(shard & 255)});\n\
    }\n\
\n\
    public void load() {\n\
        if(hdfs_output) {\n\
            $LOAD_CONF\n\
        }\n\
    }\n\
\n\
    public class Worker implements Runnable {\n\
        InputStream[] dis;\n\
        long[] f_len;\n\
        TreeMap<Key, Value> tm = null;\n\
        FileSKVWriter rfw = null;\n\
        int file_count;\n\
        int current_is;\n\
        long recs_inserted = 0;\n\
\n\
        public Worker(InputStream[] dis, int file_count, long[] f_len) {\n\
            this.tm = new TreeMap<Key, Value>();\n\
            this.file_count = file_count;\n\
            this.dis = dis;\n\
            this.f_len = f_len;\n\
            try {\n\
                if(hdfs_output) {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+Path.SEPARATOR+UUID.randomUUID()+\".rf\", fs, conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                } else {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+File.separator+UUID.randomUUID()+\".rf\", FileSystem.getLocal(conf), conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                }\n\
                this.rfw.startDefaultLocalityGroup();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
        }\n\
\n\
        @Override\n\
        public void run() {\n\
            for(int i = 0; i < this.file_count; ++i) {\n\
                this.current_is = i;\n\
\n\
                try {\n\
                    extract();\n\
                } catch(IOException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
            transform();\n\
        }\n\
\n\
        public void extract() throws IOException {\n\
            InputStream dis = this.dis[current_is];\n\
\n\
            while(f_offset[current_is] < f_len[current_is]) {\n\
                try {\n\
                    s.acquire();\n\
                } catch(InterruptedException e) {\n\
                    e.printStackTrace();\n\
                }\n\
                if(f_offset[current_is] >= f_len[current_is]) {\n\
                    s.release();\n\
                    continue;\n\
                }\n\
                $SHARD_INC\n\
                $PARSE_CONF\n\
                s.release();\n\
                $SCHEMA_CONF\n\
                if(++recs_inserted >= $MAX_RECS) {\n\
                    internalTransform();\n\
                    recs_inserted = 0;\n\
                }\n\
            }\n\
        }\n\
\n\
        public void internalTransform() {\n\
            for(Entry<Key, Value> kv : this.tm.entrySet()) {\n\
                try {\n\
                    this.rfw.append(kv.getKey(), kv.getValue());\n\
                } catch(IOException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
            try {\n\
                this.rfw.close();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            this.tm.clear();\n\
            try {\n\
                if(hdfs_output) {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+Path.SEPARATOR+UUID.randomUUID()+\".rf\", fs, conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                } else {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+File.separator+UUID.randomUUID()+\".rf\", FileSystem.getLocal(conf), conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                }\n\
                this.rfw.startDefaultLocalityGroup();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
        }\n\
\n\
        public void transform() {\n\
            for(Entry<Key, Value> kv : this.tm.entrySet()) {\n\
                try {\n\
                    this.rfw.append(kv.getKey(), kv.getValue());\n\
                } catch(IOException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
            try {\n\
                this.rfw.close();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            this.tm.clear();\n\
        }\n\
\n\
        public Text readString(InputStream dis, String delim) {\n\
            StringBuffer sb = new StringBuffer();\n\
            try {\n\
                while(sb.append((char)dis.read()).lastIndexOf(delim) == -1) { }\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            f_offset[current_is] += sb.length();\n\
            sb.deleteCharAt(sb.length() - 1);\n\
            return new Text(sb.toString().replaceAll(\"[^\\\\x20-\\\\x7e]\", \"\"));\n\
        }\n\
\n\
        public long bytesToLong(Text t) {\n\
            byte[] b = new byte[8];\n\
            byte[] tb = t.getBytes();\n\
            for(int i = 0; i < t.getLength(); ++i) {\n\
                if(i >= 8) {\n\
                    break;\n\
                }\n\
                b[i] = tb[i];\n\
            }\n\
            return (((long)(b[0] & 255) << 56) +\n\
                    ((long)(b[1] & 255) << 48) +\n\
                    ((long)(b[2] & 255) << 40) +\n\
                    ((long)(b[3] & 255) << 32) +\n\
                    ((long)(b[4] & 255) << 24) +\n\
                    ((long)(b[5] & 255) << 16) +\n\
                    ((long)(b[6] & 255) <<  8) +\n\
                    ((long)(b[7] & 255) <<  0));\n\
        }\n\
    }\n\
\n\
    public class StreamingWorker implements Runnable {\n\
        InputStream dis;\n\
        TreeMap<Key, Value> tm = null;\n\
        FileSKVWriter rfw = null;\n\
        long recs_inserted = 0;\n\
        int current_is = 0;\n\
\n\
        public StreamingWorker(InputStream dis) {\n\
            this.tm = new TreeMap<Key, Value>();\n\
            this.dis = dis;\n\
            try {\n\
                if(hdfs_output) {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+Path.SEPARATOR+UUID.randomUUID()+\".rf\", fs, conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                } else {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+File.separator+UUID.randomUUID()+\".rf\", FileSystem.getLocal(conf), conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                }\n\
                this.rfw.startDefaultLocalityGroup();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
        }\n\
\n\
        @Override\n\
        public void run() {\n\
            try {\n\
                extract();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            transform();\n\
        }\n\
\n\
        public void extract() throws IOException {\n\
            InputStream dis = this.dis;\n\
\n\
            dis.mark(1);\n\
            while(dis.read() != -1) {\n\
                dis.reset();\n\
                $SHARD_INC\n\
                $PARSE_CONF\n\
                $SCHEMA_CONF\n\
                if(++recs_inserted >= $MAX_RECS) {\n\
                    internalTransform();\n\
                    recs_inserted = 0;\n\
                }\n\
                dis.mark(1);\n\
            }\n\
        }\n\
\n\
        public void internalTransform() {\n\
            for(Entry<Key, Value> kv : this.tm.entrySet()) {\n\
                try {\n\
                    this.rfw.append(kv.getKey(), kv.getValue());\n\
                } catch(IOException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
            try {\n\
                this.rfw.close();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            this.tm.clear();\n\
            try {\n\
                if(hdfs_output) {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+Path.SEPARATOR+UUID.randomUUID()+\".rf\", fs, conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                } else {\n\
                    this.rfw = FileOperations.getInstance().openWriter(output_dir+File.separator+UUID.randomUUID()+\".rf\", FileSystem.getLocal(conf), conf, AccumuloConfiguration.getDefaultConfiguration());\n\
                }\n\
                this.rfw.startDefaultLocalityGroup();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
        }\n\
\n\
        public void transform() {\n\
            for(Entry<Key, Value> kv : this.tm.entrySet()) {\n\
                try {\n\
                    this.rfw.append(kv.getKey(), kv.getValue());\n\
                } catch(IOException e) {\n\
                    e.printStackTrace();\n\
                }\n\
            }\n\
            try {\n\
                this.rfw.close();\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            this.tm.clear();\n\
        }\n\
\n\
        public Text readString(InputStream dis, String delim) {\n\
            StringBuffer sb = new StringBuffer();\n\
            try {\n\
                while(sb.append((char)dis.read()).lastIndexOf(delim) == -1) { }\n\
            } catch(IOException e) {\n\
                e.printStackTrace();\n\
            }\n\
            f_offset[current_is] += sb.length();\n\
            sb.deleteCharAt(sb.length() - 1);\n\
            return new Text(sb.toString().replaceAll(\"[^\\\\x20-\\\\x7e]\", \"\"));\n\
        }\n\
\n\
        public long bytesToLong(Text t) {\n\
            byte[] b = new byte[8];\n\
            byte[] tb = t.getBytes();\n\
            for(int i = 0; i < t.getLength(); ++i) {\n\
                if(i >= 8) {\n\
                    break;\n\
                }\n\
                b[i] = tb[i];\n\
            }\n\
            return (((long)(b[0] & 255) << 56) +\n\
                    ((long)(b[1] & 255) << 48) +\n\
                    ((long)(b[2] & 255) << 40) +\n\
                    ((long)(b[3] & 255) << 32) +\n\
                    ((long)(b[4] & 255) << 24) +\n\
                    ((long)(b[5] & 255) << 16) +\n\
                    ((long)(b[6] & 255) <<  8) +\n\
                    ((long)(b[7] & 255) <<  0));\n\
        }\n\
    }\n\
}"
)

# Template for bulk load into a given Accumulo table
LOAD_CONF=Template("\
try {\n\
    ZooKeeperInstance inst = new ZooKeeperInstance(\"$ZOO_INSTANCE\",\"$ZOO_SERVERS\");\n\
    inst.setConfiguration(AccumuloConfiguration.getSiteConfiguration());\n\
    Connector conn = inst.getConnector(\"$USER\",\"$PASSWD\".getBytes());\n\
    if(!conn.tableOperations().exists(\"$TABLE_NAME\")) {\n\
        conn.tableOperations().create(\"$TABLE_NAME\");\n\
    }\n\
    conn.tableOperations().importDirectory(\
\"$TABLE_NAME\",\
\"$HDFS_OUTPUT_DIR\",\
\"$HDFS_OUTPUT_FAIL_DIR\",\
8,\
4,\
false);\n\
} catch(IOException e) {\n\
    e.printStackTrace();\n\
} catch(AccumuloException e) {\n\
    e.printStackTrace();\n\
} catch(AccumuloSecurityException e) {\n\
    e.printStackTrace();\n\
} catch(Exception e) {\n\
    e.printStackTrace();\n\
}\n")

# Skip a static number of bytes
ANON_LABEL=Template("\
dis.skip($const);\n\
f_offset[current_is] += $const;\n")

# Read static number of bytes
CONST_LABEL=Template("\
byte[] tmp_$label = new byte[$const];\n\
f_offset[current_is] += dis.read(tmp_$label, 0, $const);\n\
Text $label = new Text(tmp_$label);\n")

# Read a dynamic number of bytes based on delimiter
STR_LABEL=Template("\
Text $label = readString(dis, $delim);\n")

TEXT=Template("Text $var = new Text($val.getBytes());\n")

APPEND=Template("$var1.append($var2.getBytes(), 0, $var2.getLength());\n")

KEYVALUE=Template("this.tm.put(new Key($R $CF $CQ $CV $TS), new Value($VAL));\n")

HEADER_SKIP=Template("\
try {\n\
    this.dis[file_count].skip($HEADER_LEN);\n\
} catch(IOException e) {\n\
    e.printStackTrace();\n\
}\n\
this.f_offset[file_count] = $HEADER_LEN;\n")
