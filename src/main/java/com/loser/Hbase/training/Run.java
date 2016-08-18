package com.loser.Hbase.training;

import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }
        switch (args[0]) {
            case "STM":
                SparkTrafficMonitor.main(args);
                break;
            case "ITM":
                InfoTrafficMonitor.main(args);
                break;
            case "both":
            	monitor_both.main(args);
                break;
            case "SFC":
                SplitFlowConsumer.main(args);
                break;
            case "SFCWP":
            	SplitFlowConsumerWP.main(args);
                break;
            case "SFCWProb":
            	SplitFlowConsumerWProb.main(args);
                break;
            case "both_tag":
            	Monitor_taglatency.main(args);
            	break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}
