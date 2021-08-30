package in.dreamlab.wicm.conf;

import org.apache.giraph.conf.IntConfOption;

@SuppressWarnings("rawtypes")
public interface WICMConstants {
    IntConfOption BUFFER_SIZE = new IntConfOption("wicm.localBufferSize", 500,
                                            "Local Buffer size to use in block based warp");
    IntConfOption MIN_MESSAGES = new IntConfOption("wicm.minMessages", 50,
                                            "minimum messages to use in block based warp");
    // one graph one algorithm iterate over different values
}

// breakup
// 1 - page introduction, motivation, (+1/2)related work
// 1 - archteure / design/ methods
// 1/2 - proof intuition
// 1-1 1/2 - results
// 1/2 results