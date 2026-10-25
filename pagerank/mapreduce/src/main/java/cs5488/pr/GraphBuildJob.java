package cs5488.pr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * MR2: 共现边 -> PageRank 节点记录 (邻接表格式), 含 Combiner 局部聚合
 *
 * 输入 : "i1 \t i2 \t w"   (MR1 输出, 无向边, 可能出现重复边)
 * 输出 : "id \t mass \t adj"  mass 初始为 1.0, adj = "nbr1:w1 nbr2:w2 ..."
 *
 * Combiner: map 端先按 (id,nbr) 求和, 显著降低 shuffle 量
 * Counter : NODE_COUNT = 图节点总数 N (供驱动做 PR=mass/N 归一化)
 */
public class GraphBuildJob extends Configured implements Tool {

    public enum Counters { NODE_COUNT }

    /** Mapper: 无向边双向发射  id -> "nbr:w" */
    public static class EdgeMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 3) {
                return;
            }
            outKey.set(f[0]);
            outVal.set(f[1] + ":" + f[2]);
            ctx.write(outKey, outVal);
            outKey.set(f[1]);
            outVal.set(f[0] + ":" + f[2]);
            ctx.write(outKey, outVal);
        }
    }

    /** Combiner: 同一 id 下相同 nbr 的权重求和 (类型与 map 输出一致, 可用) */
    public static class EdgeCombiner
            extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            Map<String, Long> agg = new HashMap<>();
            for (Text v : values) {
                String s = v.toString();
                int c = s.lastIndexOf(':');
                String nbr = s.substring(0, c);
                long w = Long.parseLong(s.substring(c + 1));
                Long old = agg.get(nbr);
                agg.put(nbr, old == null ? w : old + w);
            }
            for (Map.Entry<String, Long> e : agg.entrySet()) {
                outVal.set(e.getKey() + ":" + e.getValue());
                ctx.write(key, outVal);
            }
        }
    }

    /** Reducer: 汇总每个节点的邻接表, 发射节点记录 */
    public static class NodeReducer
            extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            Map<String, Long> adj = new HashMap<>();
            for (Text v : values) {
                String s = v.toString();
                int c = s.lastIndexOf(':');
                String nbr = s.substring(0, c);
                long w = Long.parseLong(s.substring(c + 1));
                Long old = adj.get(nbr);
                adj.put(nbr, old == null ? w : old + w);
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Long> e : adj.entrySet()) {
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                sb.append(e.getKey()).append(':').append(e.getValue());
            }
            outVal.set("1.0\t" + sb);        // mass=1.0, 总和=N, PR=mass/N
            ctx.write(key, outVal);
            ctx.getCounter(Counters.NODE_COUNT).increment(1);
        }
    }

    public static long runJob(String input, String output, Configuration baseConf)
            throws Exception {
        Job job = Job.getInstance(baseConf, "MR2-graph-build");
        job.setJarByClass(GraphBuildJob.class);
        job.setMapperClass(EdgeMapper.class);
        job.setCombinerClass(EdgeCombiner.class);
        job.setReducerClass(NodeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        if (!job.waitForCompletion(true)) {
            throw new IOException("MR2 graph build failed");
        }
        return job.getCounters()
                .findCounter(Counters.NODE_COUNT).getValue();
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: GraphBuildJob <edgesDir> <graphDir>");
            return 2;
        }
        long n = runJob(args[0], args[1], getConf());
        System.out.println("NODE_COUNT=" + n);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),
                new GraphBuildJob(), args));
    }
}