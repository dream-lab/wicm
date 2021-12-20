package in.dreamlab.wicm.graph.mutations;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.conf.WICMConstants;
import in.dreamlab.wicm.graph.computation.DebugDeferredWindowIntervalComputation;
import in.dreamlab.wicm.graph.computation.DebugWindowIntervalComputation;
import in.dreamlab.wicm.io.mutations.WICMMutationFileReader;
import in.dreamlab.wicm.types.VarIntWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * We mark vertex active if some state present in future to update
 */
public abstract class DebugWICMMutationsIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends DebugDeferredWindowIntervalComputation<I, T, S, V, EP, E, PW, P, IM> implements WICMConstants{
    protected static Logger LOG = Logger.getLogger(DebugWICMMutationsIntervalComputation.class);

    protected boolean isMutation;

    private void readMutationFile(String path) throws IOException {
        WICMMutationFileReader<I,V,E> mutationReader = MUTATION_READER_CLASS.newInstance(getConf());
        try {
            mutationReader.initialise(path);
            while (mutationReader.hasNext()) {
                switch (mutationReader.getMode()) {
                    case ADD_VERTEX:
                        OutEdges<I, E> mutatedEdges = getConf().createOutEdges();
                        mutatedEdges.initialize(mutationReader.getEdges());
                        /*LOG.info("Adding a vertex: "+mutationReader.getVertexId().toString()+","
                                +mutationReader.getVertexValue().getProperty().toString() +","+mutatedEdges);*/
                         addVertexRequest(mutationReader.getVertexId(), mutationReader.getVertexValue(), mutatedEdges);
                        break;
                    case TRUNCATE_VERTEX:
                        /*LOG.info("Truncating a vertex: "+mutationReader.getVertexId().toString()+","
                                +mutationReader.getVertexValue().getLifespan().toString());*/
                         addVertexRequest(mutationReader.getVertexId(), mutationReader.getVertexValue());
                        break;
                    case DELETE_VERTEX:
                        // LOG.info("Deleting a vertex: "+mutationReader.getVertexId().toString());
                        removeVertexRequest(mutationReader.getVertexId());
                        break;
                    case REPLACE_EDGE:
                        // LOG.info("Replacing edge: "+mutationReader.getEdges().toString());
                        for(Edge<I,E> e : mutationReader.getEdges()) {
                            addEdgeRequest(mutationReader.getVertexId(), e);
                        }
                        break;
                    default:
                        LOG.info("Invalid mode");
                        break;
                }
            }
        } finally {
            mutationReader.close();
        }
    }

    @Override
    public void preSuperstep() {
        if (!isMutation) {
            super.preSuperstep();
        } else {
            WICMMutationsWorkerContext castedWorker = getWorkerContext();
            boolean lockAcquired = castedWorker.fileLock.tryAcquire();
            if(lockAcquired) {
                try {
                    readMutationFile(castedWorker.mutationFileName);
                } catch (IOException e) {
                    LOG.info("Caught exception",e);
                }
                castedWorker.fileLock.release();
            }
        }
    }

    @Override
    public void postSuperstep() {
        if (!isMutation) {
            super.postSuperstep();
        } else {
            WICMMutationsWorkerContext castedWorker = getWorkerContext();
            castedWorker.finished.set(true);
        }
    }

    @Override
    public void compute(Vertex<I, V, E> vertex, Iterable<IM> messages) throws IOException {
        if(!isMutation) {
            super.compute(vertex,messages);
        }

        // dummy vertex remains active
        if(isInitial || (getSuperstep() == 0)) {
            if(vertex.getId().getClass().equals(IntWritable.class) && ((IntWritable) vertex.getId()).get() == -1)
                vertex.wakeUp();
            else if(vertex.getId().getClass().equals(VarIntWritable.class) && ((VarIntWritable) vertex.getId()).get() == -1)
                vertex.wakeUp();
        }
    }
}
