package cs5488.pr;

import java.io.IOException;

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
 * MR3: 单轮 PageRank 迭代 (Lin & Dyer 质量传递法)
 *
 * 节点记录: "id \t mass \t adj",  adj = "nbr:w ..."
 * 更新公式: new_mass(i) = (1-d) + d * SUM_j mass(j)*w(j,i)/S(j),  S(j)=j的边权和
 * 全图无悬挂节点 (图由边构建, 每个节点至少1条边), 故无需 dangling 修正;
 * 质量总和恒为 N, 最终 PR(i) = mass(i)/N.
 *
 * Counter: DELTA_MICRO = (SUM |new_mass-old_mass| / N) * 1e9
 *   除以 N 是为了与单机 Python 版口径一致: L1 delta 定义在 **PR 尺度**上,
 *   而不是 mass 尺度。否则同样写 tol=1e-6, MR 版的实际严格程度是单机版的 N 倍
 *   (因为 mass = N * PR), 收敛轮数会对不上。
 */
public class PageRankJob extends Configured implements Tool {

    public enum Counters { DELTA_MICRO, MASS_MICRO, DANGLING }

    public static final String VALUE_NODE = "N";   // 节点记录透传标记
    public static final String VALUE_MASS = "M";   // 质量贡献标记

    /** Mapper: 透传节点记录 + 沿邻接表分发质量 */
    public static class MassMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private double damping;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            damping = ctx.getConfiguration().getFloat("pr.damping", 0.85f);
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 3) {
                return;
            }
            String id = f[0];
            double mass = Double.parseDouble(f[1]);
            String adj = f[2];

            // 1) 透传节点记录 (保留邻接表到下一轮)
            outKey.set(id);
            outVal.set(VALUE_NODE + "\t" + f[1] + "\t" + adj);
            ctx.write(outKey, outVal);

            // 2) 分发质量: 每个邻居 j 得到 mass * d * w_ij / S_i
            String[] toks = adj.split(" ");
            double strength = 0.0;
            for (String t : toks) {
                strength += Double.parseDouble(t.substring(t.lastIndexOf(':') + 1));
            }
            if (strength <= 0) {
                ctx.getCounter(Counters.DANGLING).increment(1);
                return;
            }
            double share = mass * damping / strength;
            for (String t : toks) {
                int c = t.lastIndexOf(':');
                outKey.set(t.substring(0, c));
                outVal.set(VALUE_MASS + "\t"
                        + (share * Double.parseDouble(t.substring(c + 1))));
                ctx.write(outKey, outVal);
            }
        }
    }
    /** Reducer: 汇总入质量, 计算 new_mass = (1-d) + incoming */
    public static class MassReducer
            extends Reducer<Text, Text, Text, Text> {
        private double damping;
        private double nodeCount;
        private double localDelta = 0.0;
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            damping = conf.getFloat("pr.damping", 0.85f);
            nodeCount = conf.getDouble("pr.node.count", 1.0);
            if (nodeCount <= 0) {
                nodeCount = 1.0;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            double incoming = 0.0;
            double oldMass = 0.0;
            String adj = null;
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith(VALUE_NODE)) {
                    int t1 = s.indexOf('\t');
                    int t2 = s.indexOf('\t', t1 + 1);
                    oldMass = Double.parseDouble(s.substring(t1 + 1, t2));
                    adj = s.substring(t2 + 1);
                } else {   // VALUE_MASS
                    incoming += Double.parseDouble(s.substring(s.indexOf('\t') + 1));
                }
            }
            if (adj == null) {
                return;   // 只有贡献没有节点记录 (理论上不发生)
            }
            double newMass = (1.0 - damping) + incoming;
            outVal.set(newMass + "\t" + adj);
            ctx.write(key, outVal);
            // 先在 reducer 内累加, cleanup 时一次性按 /N 上报计数器,
            // 避免逐条 Math.round(.. /N * 1e9) 在小 delta 时被全部舍入成 0
            localDelta += Math.abs(newMass - oldMass);
            ctx.getCounter(Counters.MASS_MICRO)
                    .increment(Math.round(newMass * 1e6));
        }

        @Override
        protected void cleanup(Context ctx) {
            ctx.getCounter(Counters.DELTA_MICRO)
                    .increment(Math.round(localDelta / nodeCount * 1e9));
        }
    }

    /** @return 本轮 L1 delta = SUM|new_mass-old_mass| / N  (PR 尺度, 与单机版同口径) */
    public static double runJob(String input, String output, double damping,
            double nodeCount, Configuration baseConf) throws Exception {
        Job job = Job.getInstance(baseConf, "MR3-pagerank-iteration");
        job.setJarByClass(PageRankJob.class);
        job.getConfiguration().setFloat("pr.damping", (float) damping);
        job.getConfiguration().setDouble("pr.node.count", nodeCount);
        job.setMapperClass(MassMapper.class);
        job.setReducerClass(MassReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        if (!job.waitForCompletion(true)) {
            throw new IOException("MR3 pagerank iteration failed");
        }
        return job.getCounters()
                .findCounter(Counters.DELTA_MICRO).getValue() / 1e9;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: PageRankJob <graph_k-1> <graph_k> [nodeCount=1] [damping=0.85]");
            System.err.println("  nodeCount 用于把 L1 delta 归一到 PR 尺度, 单独调试时可填 1");
            return 2;
        }
        double n = args.length > 2 ? Double.parseDouble(args[2]) : 1.0;
        double d = args.length > 3 ? Double.parseDouble(args[3]) : 0.85;
        double delta = runJob(args[0], args[1], d, n, getConf());
        System.out.println("L1_DELTA=" + delta);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),
                new PageRankJob(), args));
    }
}