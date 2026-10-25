package cs5488.pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 总驱动: 淘宝 UserBehavior -> 会话共现图 -> 迭代 PageRank -> Top-K 枢纽
 *
 * 用法:
 *   hadoop jar taobao-pagerank-1.0.jar cs5488.pr.PageRankDriver \
 *       <inputCsv> <outBaseDir> [sampleMod] [sessionGap] [damping] \
 *       [maxIter] [tol] [topK] [behaviors]
 *
 * 例 (伪分布式, 330 采样, 与本地 python 结果对齐):
 *   hadoop jar taobao-pagerank-1.0.jar cs5488.pr.PageRankDriver \
 *       /user/hadoop/taobao/UserBehavior.csv /user/hadoop/taobao/pr \
 *       330 1800 0.85 80 1e-6 30 pv,buy,cart,fav
 *
 * 阶段:
 *   MR1 CooccurrenceJob  二次排序会话化建边      -> <out>/edges
 *   MR2 GraphBuildJob    边 -> 邻接表节点记录     -> <out>/graph_0   (得到 N)
 *   MR3 PageRankJob x k  迭代至收敛 (Counter 判停) -> <out>/graph_k
 *   MR4 TopKJob          in-mapper Top-K 全局排名  -> <out>/topk
 */
public class PageRankDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRankDriver <inputCsv> <outBaseDir> "
                    + "[sampleMod=330] [sessionGap=1800] [damping=0.85] "
                    + "[maxIter=50] [tol=1e-6] [topK=30] "
                    + "[behaviors=pv,buy,cart,fav]");
            return 2;
        }
        String input = args[0];
        String outBase = args[1];
        int sampleMod = args.length > 2 ? Integer.parseInt(args[2]) : 330;
        long sessionGap = args.length > 3 ? Long.parseLong(args[3]) : 1800L;
        double damping = args.length > 4 ? Double.parseDouble(args[4]) : 0.85;
        int maxIter = args.length > 5 ? Integer.parseInt(args[5]) : 50;
        double tol = args.length > 6 ? Double.parseDouble(args[6]) : 1e-6;
        int topK = args.length > 7 ? Integer.parseInt(args[7]) : 30;
        String[] behaviors = args.length > 8 ? args[8].split(",")
                : new String[] { "pv", "buy", "cart", "fav" };

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        long t0 = System.currentTimeMillis();

        // ---- MR1: 会话化建边 -------------------------------------------
        String edgesDir = outBase + "/edges";
        fs.delete(new Path(edgesDir), true);
        System.out.println("[MR1] cooccurrence edges -> " + edgesDir);
        if (!CooccurrenceJob.runJob(input, edgesDir, sampleMod, sessionGap,
                50, behaviors, conf)) {
            System.err.println("MR1 failed");
            return 1;
        }

        // ---- MR2: 建图 --------------------------------------------------
        String graph0 = outBase + "/graph_0";
        fs.delete(new Path(graph0), true);
        System.out.println("[MR2] build adjacency -> " + graph0);
        long nodeCount = GraphBuildJob.runJob(edgesDir, graph0, conf);
        System.out.println("[MR2] N = " + nodeCount);

        // ---- MR3: 迭代 PageRank ------------------------------------------
        int iter = 0;
        double delta = Double.MAX_VALUE;
        while (iter < maxIter && delta > tol) {
            String in = outBase + "/graph_" + iter;
            String out = outBase + "/graph_" + (iter + 1);
            fs.delete(new Path(out), true);
            delta = PageRankJob.runJob(in, out, damping, (double) nodeCount, conf);
            iter++;
            System.out.printf("[MR3] iter=%d  L1_delta=%.6e  (tol=%.0e)%n",
                    iter, delta, tol);
            // 只保留最近两轮, 节省 HDFS 空间
            if (iter >= 2) {
                fs.delete(new Path(outBase + "/graph_" + (iter - 2)), true);
            }
        }

        // ---- MR4: Top-K ---------------------------------------------------
        String finalGraph = outBase + "/graph_" + iter;
        String topkDir = outBase + "/topk";
        fs.delete(new Path(topkDir), true);
        System.out.println("[MR4] top-" + topK + " -> " + topkDir);
        if (!TopKJob.runJob(finalGraph, topkDir, nodeCount, topK, conf)) {
            System.err.println("MR4 failed");
            return 1;
        }

        long ms = System.currentTimeMillis() - t0;
        System.out.println("=============================================");
        System.out.printf("DONE  iters=%d  N=%d  final_L1=%.3e  wall=%.1fs%n",
                iter, nodeCount, delta, ms / 1000.0);
        System.out.println("Top-K: hdfs dfs -cat " + topkDir + "/part-r-00000");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),
                new PageRankDriver(), args));
    }
}