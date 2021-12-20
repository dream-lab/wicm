package in.dreamlab.wicm.conf;

import in.dreamlab.wicm.io.mutations.WICMMutationFileReader;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;

@SuppressWarnings("rawtypes")
public interface WICMConstants {
    IntConfOption BUFFER_SIZE = new IntConfOption("wicm.localBufferSize", 500,
                                            "Local Buffer size to use in block based warp");
    IntConfOption MIN_MESSAGES = new IntConfOption("wicm.minMessages", 50,
                                            "minimum messages to use in block based warp");
//    IntConfOption MAX_MESSAGES = new IntConfOption("wicm.maxMessages", 100,
//            "minimum messages to use in block based warp");
    BooleanConfOption ENABLE_BLOCK = new BooleanConfOption("icm.blockWarp", false,
            "Enable Block Based Warp");

    ResolverDumpOutput resolverOutput = new ResolverDumpOutput();
    StrConfOption MUTATION_PATH = new StrConfOption("wicm.mutationPath", "dump", "Path for mutation files");
    StrConfOption RESOLVER_SUBDIR = new StrConfOption("wicm.resolverPath", "dump", "Path for resolver dump");
    ClassConfOption<WICMMutationFileReader> MUTATION_READER_CLASS = ClassConfOption.create("wicm.mutationReaderClass", null,
            WICMMutationFileReader.class, "Mutation Reader class");
}