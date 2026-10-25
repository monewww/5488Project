package cs5488.pr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR4: 全局 Top-K 枢纽商品 (in-mapper Top-K 模式)
 *
 * 输入 : 收敛后的节点记录 "id \t mass \t adj"
 * 输出 : "rank \t id \t pagerank \t strength"  (pagerank = mass / N)
 *
 * Mapper 端各自维护大小为 k 的 TreeMap (in-mapper top-k),
 * 只把每个 map task 的前 k 名发给唯一 reducer 做全局归并,
 * 避免把 168,000 条记录全部 shuffle 到 1 个 reducer。
 */
public class TopKJob extends Configured implements Tool {

    /** Mapper: 维护局部 Top-K, cleanup 时统一发射 (key=-pr 借框架排序) */
    public static class TopKMapper
            extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private int k;
        private double nodeCount;
        private final TreeMap<Double, List<String>> top = new TreeMap<>();
        private int size = 0;
        private final DoubleWritable outKey = new DoubleWritable();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            k = conf.getInt("pr.topk", 30);
            nodeCount = conf.getDouble("pr.node.count", 1.0);
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 3) {
                return;
            }
            double pr = Double.parseDouble(f[1]) / nodeCount;
            double strength = 0.0;
            for (String t : f[2].split(" ")) {
                strength += Double.parseDouble(t.substring(t.lastIndexOf(':') + 1));
            }
            top.computeIfAbsent(pr, x -> new ArrayList<>())
                    .add(f[0] + "\t" + pr + "\t" + (long) strength);
            size++;
            while (size > k) {           // 淘汰最小的
                Map.Entry<Double, List<String>> last = top.lastEntry();
                List<String> lst = last.getValue();
                lst.remove(lst.size() - 1);
                size--;
                if (lst.isEmpty()) {
                    top.remove(last.getKey());
                }
            }
        }

        @Override
        protected void cleanup(Context ctx)
                throws IOException, InterruptedException {
            for (Map.Entry<Double, List<String>> e : top.entrySet()) {
                for (String rec : e.getValue()) {
                    // key = -pr: 框架按键升序排序 => reducer 收到时即按 pr 降序
                    outKey.set(-e.getKey());
                    outVal.set(rec);
                    ctx.write(outKey, outVal);
                }
            }
        }
    }

    /** Reducer (1个): 按 pr 降序到达, 编号取前 k 名 */
    public static class TopKReducer
            extends Reducer<DoubleWritable, Text, Text, Text> {
        private int k;
        private int rank = 0;
        private final Text outKey = new Text();

        @Override
        protected void setup(Context ctx) {
            k = ctx.getConfiguration().getInt("pr.topk", 30);
        }

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values,
                Context ctx) throws IOException, InterruptedException {
            for (Text v : values) {
                if (rank >= k) {
                    return;
                }
                rank++;
                outKey.set(String.valueOf(rank));
                ctx.write(outKey, v);   // 行: "rank \t id \t pr \t strength"
            }
        }
    }

    public static boolean runJob(String input, String output,
            double nodeCount, int k, Configuration baseConf) throws Exception {
        Job job = Job.getInstance(baseConf, "MR4-topk");
        job.setJarByClass(TopKJob.class);
        job.getConfiguration().setInt("pr.topk", k);
        job.getConfiguration().setDouble("pr.node.count", nodeCount);
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(DoubleWritable.class);
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
        if (args.length < 4) {
            System.err.println(
                    "Usage: TopKJob <graphFinal> <topkDir> <nodeCount> [k=30]");
            return 2;
        }
        double n = Double.parseDouble(args[2]);
        int k = args.length > 3 ? Integer.parseInt(args[3]) : 30;
        return runJob(args[0], args[1], n, k, getConf()) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TopKJob(), args));
    }
}