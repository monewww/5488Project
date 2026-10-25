package cs5488.pr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR1: 会话化 + 商品共现建边 (二次排序 secondary sort 演示)
 *
 * 输入 : UserBehavior.csv 原始行 "uid,item,cat,beh,ts"
 * 输出 : 共现边 "i1 \t i2 \t w"  (i1<i2, w=该用户对(i1,i2)的共现会话次数)
 *
 * 二次排序三件套:
 *   Key      = (uid, ts)          复合键
 *   Partition= 只按 uid 分区       同一用户全部记录进同一 reducer
 *   Sort     = uid 升序, ts 升序   reducer 内按时间有序
 *   Group    = 只按 uid 分组       一次 reduce() 收到一个用户的全部事件
 */
public class CooccurrenceJob extends Configured implements Tool {

    /** 复合键 (uid, ts) */
    public static class UserTsKey implements WritableComparable<UserTsKey> {
        private int uid;
        private long ts;

        public UserTsKey() { }
        public UserTsKey(int uid, long ts) { this.uid = uid; this.ts = ts; }

        public int getUid() { return uid; }
        public long getTs() { return ts; }

        public void set(int uid, long ts) { this.uid = uid; this.ts = ts; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(uid);
            out.writeLong(ts);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            uid = in.readInt();
            ts = in.readLong();
        }

        @Override
        public int compareTo(UserTsKey o) {
            int c = Integer.compare(uid, o.uid);
            return c != 0 ? c : Long.compare(ts, o.ts);
        }

        @Override
        public int hashCode() { return uid * 163; }   // 只按 uid 哈希 -> 配合分区器

        @Override
        public boolean equals(Object o) {
            return o instanceof UserTsKey && ((UserTsKey) o).uid == uid;
        }

        @Override
        public String toString() { return uid + "\t" + ts; }
    }

    /** 分区器: 只按 uid */
    public static class UidPartitioner extends Partitioner<UserTsKey, Text> {
        @Override
        public int getPartition(UserTsKey key, Text value, int numPartitions) {
            return (key.getUid() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /** 排序比较器: uid 升序, ts 升序 (完整比较) */
    public static class FullKeyComparator extends WritableComparator {
        protected FullKeyComparator() { super(UserTsKey.class, true); }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UserTsKey k1 = (UserTsKey) a;
            UserTsKey k2 = (UserTsKey) b;
            return k1.compareTo(k2);
        }
    }

    /** 分组比较器: 只按 uid, 使一个用户的所有 (uid,ts) 键进同一次 reduce() */
    public static class UidGroupComparator extends WritableComparator {
        protected UidGroupComparator() { super(UserTsKey.class, true); }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UserTsKey k1 = (UserTsKey) a;
            UserTsKey k2 = (UserTsKey) b;
            return Integer.compare(k1.getUid(), k2.getUid());
        }
    }

    /** Mapper: 过滤行为类型 + hash 采样, 发射 (uid,ts) -> "ts\titem" */
    public static class EventMapper
            extends Mapper<LongWritable, Text, UserTsKey, Text> {
        private int sampleMod;
        private String[] behaviors;
        private final UserTsKey outKey = new UserTsKey();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            sampleMod = conf.getInt("ub.sample.mod", 330);
            behaviors = conf.getStrings("ub.behaviors",
                    new String[] { "pv", "buy", "cart", "fav" });
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split(",");
            if (f.length < 5) {
                return;
            }
            String beh = f[3].trim();
            if (!Arrays.asList(behaviors).contains(beh)) {
                return;
            }
            int uid;
            long ts;
            try {
                uid = Integer.parseInt(f[0].trim());
                ts = Long.parseLong(f[4].trim());
            } catch (NumberFormatException e) {
                return;   // 跳过坏行
            }
            if (sampleMod > 1 && Math.floorMod(uid, sampleMod) != 0) {
                return;   // hash 采样
            }
            outKey.set(uid, ts);
            // 值里带 ts: 分组比较器只按 uid 分组后, reduce() 里仍能取到每条记录的时间
            outVal.set(ts + "\t" + f[1].trim());
            ctx.write(outKey, outVal);
        }
    }
    /** Reducer: 时间有序事件流 -> 会话切分 -> 用户内共现对聚合 -> 发射边 */
    public static class SessionReducer
            extends Reducer<UserTsKey, Text, Text, Text> {
        private long sessionGap;
        private int maxItems;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            sessionGap = conf.getLong("ub.session.gap", 1800L);
            maxItems = conf.getInt("ub.max.session.items", 50);
        }

        /** 会话内商品两两配对, 累加到用户级聚合表 (map-side local aggregation) */
        private void flushSession(LinkedHashSet<String> session,
                                  Map<String, Long> pairCount) {
            List<String> items = new ArrayList<>(session);
            if (items.size() > maxItems) {
                items = items.subList(0, maxItems);   // 截断超大会话, 防 O(n^2)
            }
            for (int i = 0; i < items.size(); i++) {
                for (int j = i + 1; j < items.size(); j++) {
                    String a = items.get(i);
                    String b = items.get(j);
                    if (a.compareTo(b) > 0) {
                        String t = a; a = b; b = t;
                    }
                    String pk = a + " " + b;
                    Long c = pairCount.get(pk);
                    pairCount.put(pk, c == null ? 1L : c + 1L);
                }
            }
        }

        @Override
        protected void reduce(UserTsKey key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            LinkedHashSet<String> session = new LinkedHashSet<>();
            Map<String, Long> pairCount = new HashMap<>();
            long lastTs = Long.MIN_VALUE;
            for (Text v : values) {
                String s = v.toString();
                int tab = s.indexOf('\t');
                long ts = Long.parseLong(s.substring(0, tab));
                String item = s.substring(tab + 1);
                if (lastTs != Long.MIN_VALUE && ts - lastTs > sessionGap) {
                    flushSession(session, pairCount);
                    session.clear();
                }
                session.add(item);
                lastTs = ts;
            }
            flushSession(session, pairCount);
            for (Map.Entry<String, Long> e : pairCount.entrySet()) {
                int sp = e.getKey().indexOf(' ');
                outKey.set(e.getKey().substring(0, sp));
                outVal.set(e.getKey().substring(sp + 1) + "\t" + e.getValue());
                ctx.write(outKey, outVal);   // 行: "i1 \t i2 \t w"
            }
        }
    }

    public static boolean runJob(String input, String output, int sampleMod,
            long sessionGap, int maxItems, String[] behaviors,
            Configuration baseConf) throws Exception {
        Job job = Job.getInstance(baseConf, "MR1-cooccurrence-secondary-sort");
        job.setJarByClass(CooccurrenceJob.class);
        Configuration conf = job.getConfiguration();
        conf.setInt("ub.sample.mod", sampleMod);
        conf.setLong("ub.session.gap", sessionGap);
        conf.setInt("ub.max.session.items", maxItems);
        conf.setStrings("ub.behaviors", behaviors);

        job.setMapperClass(EventMapper.class);
        job.setReducerClass(SessionReducer.class);
        job.setPartitionerClass(UidPartitioner.class);
        job.setSortComparatorClass(FullKeyComparator.class);
        job.setGroupingComparatorClass(UidGroupComparator.class);

        job.setMapOutputKeyClass(UserTsKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                "Usage: CooccurrenceJob <input.csv> <edgesDir> "
                + "[sampleMod=330] [sessionGap=1800] [maxItems=50] [behaviors=pv,buy,cart,fav]");
            return 2;
        }
        int sampleMod = args.length > 2 ? Integer.parseInt(args[2]) : 330;
        long sessionGap = args.length > 3 ? Long.parseLong(args[3]) : 1800L;
        int maxItems = args.length > 4 ? Integer.parseInt(args[4]) : 50;
        String[] behaviors = args.length > 5 ? args[5].split(",")
                : new String[] { "pv", "buy", "cart", "fav" };
        return runJob(args[0], args[1], sampleMod, sessionGap, maxItems,
                behaviors, getConf()) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),
                new CooccurrenceJob(), args));
    }
}