package in.dreamlab.wicm.conf;

import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.IntConfOption;

@SuppressWarnings("rawtypes")
public interface WICMConstants {
    IntConfOption BUFFER_SIZE = new IntConfOption("wicm.localBufferSize", 500,
                                            "Local Buffer size to use in block based warp");
    IntConfOption MIN_MESSAGES = new IntConfOption("wicm.minMessages", 50,
                                            "minimum messages to use in block based warp");
    IntConfOption MAX_MESSAGES = new IntConfOption("wicm.maxMessages", 100,
            "minimum messages to use in block based warp");
    BooleanConfOption ENABLE_BLOCK = new BooleanConfOption("icm.blockWarp", false,
            "Enable Block Based Warp");
}