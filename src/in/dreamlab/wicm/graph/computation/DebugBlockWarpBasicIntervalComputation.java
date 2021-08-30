package in.dreamlab.wicm.graph.computation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graph.computation.BasicIntervalComputation;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.conf.WICMConstants;
import in.dreamlab.wicm.utils.LocalWritableMessageBuffer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class DebugBlockWarpBasicIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends BasicIntervalComputation<I, T, S, V, EP, E, PW, P, IM> implements WICMConstants {
    private static Logger LOG = Logger.getLogger(DebugBlockWarpBasicIntervalComputation.class);

    private RangeMap<T, S> changedStates;
    private LocalWritableMessageBuffer<IM> messageBuffer; // do lazy initialisation instead !

    private PerfDebugData localDebugData = new PerfDebugData();
    private final BooleanConfOption dumpPerfData = new BooleanConfOption("debugPerformance", false, "Collect debug metrics");

    @Override
    public void preSuperstep() {
        LOG.info("Creating buffer with size "+BUFFER_SIZE.get(getConf())+" and min messages "+MIN_MESSAGES.get(getConf()));
        messageBuffer = new LocalWritableMessageBuffer<>(BUFFER_SIZE.get(getConf()), getConf().getOutgoingMessageValueClass());
        changedStates = TreeRangeMap.create();
        localDebugData.initialise();
    }

    @Override
    public void postSuperstep() {
        if(dumpPerfData.get(getConf())) {
            localDebugData.dump(LOG, getSuperstep());
        }
    }

    @Override
    public void compute(Vertex<I, V, E> vertex, Iterable<IM> messages) throws IOException {
        long intervalComputeRegion = System.nanoTime();
        intervalCompute((IntervalVertex<I, T, S, V, EP, E, PW, P, IM>) vertex, messages);
        intervalComputeRegion = System.nanoTime() - intervalComputeRegion;
        localDebugData.ICRegion += intervalComputeRegion;
        localDebugData.giraphCompCalls += 1;
    }

    public void intervalCompute(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Iterable<IM> messages)
            throws IOException {
        long timedRegion;
        if (getSuperstep() == 0) {
            /** Initialize, called by infrastructure before the superstep 0 starts. */
            timedRegion = System.nanoTime();
            boolean doScatter = init(intervalVertex);
            timedRegion = System.nanoTime() - timedRegion;
            localDebugData.InitRegion += timedRegion;

            if (doScatter) {
                for (Map.Entry<Range<T>, S> vertexState : intervalVertex.getStateEntrySet()) {
                    timedRegion = System.nanoTime();
                    _scatter(intervalVertex, intervalVertex.createInterval(vertexState.getKey()), vertexState.getValue());
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData._SRegion += timedRegion;
                }
            }
        } else {
            int messageSize = Iterables.size(messages);
            int index = Integer.min((messageSize-1)/10, 499);
            localDebugData.WarpMessageDistribution[index] += 1;

            // process messages in blocks
            int numBlocks = 1, blockSize = messageSize;
            if (messageSize >= MIN_MESSAGES.get(getConf())) {
                numBlocks = (int) Math.floor(Math.sqrt(messageSize));
                blockSize = Integer.min(BUFFER_SIZE.get(getConf()), ((int) Math.ceil(messageSize * 1. / numBlocks)));
            }

            changedStates.clear();
            messageBuffer.reset();
            for(IM m : messages) {
                messageBuffer.addMessage(m);
                if(messageBuffer.filledBufferSize() == blockSize) {
                    localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                    int newStates = intervalVertex.getState().size();
                    localDebugData.NewWarpCost += ((blockSize+newStates)*Math.log((blockSize+newStates)));
                    warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
                    messageBuffer.reset();
                }
            }

            if(!messageBuffer.isEmpty()) {
                localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                int newStates = intervalVertex.getState().size();
                localDebugData.NewWarpCost += ((blockSize+newStates)*Math.log((blockSize+newStates)));
                warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
            }

            /** User can choose to update state only for a sub-interval, this results into interval fragementation */
            for(Map.Entry<Range<T>, S> updatedIntervalState : changedStates.asMapOfRanges().entrySet()) {
                Interval<T> stateInterval = intervalVertex.createInterval(updatedIntervalState.getKey());
                timedRegion = System.nanoTime();
                _scatter(intervalVertex, stateInterval, updatedIntervalState.getValue());
                timedRegion = System.nanoTime() - timedRegion;
                localDebugData._SRegion += timedRegion;
                localDebugData.RUpdate += stateInterval.getLength();
            }
        }

        if(!intervalVertex.hasVotedToRemainActive())
            intervalVertex.voteToHalt();
    }

    protected void _scatter(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Interval<T> interval, S vState) {
        long timedRegion;
        for (Edge<I, E> edge : intervalVertex.getEdges()) {
            /** Scatter-Side Warp*/
            Interval<T> intersection = edge.getValue().getLifespan().getIntersection(interval);
            if (intersection!=null) {
                if(getPropertyLabelForScatter()!=null) {
                    /** State can span multiple fragemented sub-intervals of a single edge */
                    for(Map.Entry<Range<T>, EP> edgeSubInterval : edge.getValue().getProperty(getPropertyLabelForScatter(), intersection)) {
                        timedRegion = System.nanoTime();
                        Iterable<IM> messages = scatter(intervalVertex, edge, (Interval<T>) intervalVertex.createInterval(edgeSubInterval.getKey()), vState, edgeSubInterval.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.SRegion += timedRegion;
                        localDebugData.ScattCalls += 1;

                        /** User decides if message should be sent along an edge */
                        timedRegion = System.nanoTime();
                        for(IM msg : messages) {
                            sendMessage(edge.getTargetVertexId(), msg);
                        }
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.MsgSerRegion += timedRegion;
                        localDebugData.Messages += Iterables.size(messages);
                    }
                } else {
                    timedRegion = System.nanoTime();
                    Iterable<IM> messages = scatter(intervalVertex, edge, intersection, vState, null);
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.SRegion += timedRegion;
                    localDebugData.ScattCalls += 1;

                    /** User decides if message should be sent along an edge */
                    timedRegion = System.nanoTime();
                    for(IM msg : messages) {
                        sendMessage(edge.getTargetVertexId(), msg);
                    }
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.MsgSerRegion += timedRegion;
                    localDebugData.Messages += Iterables.size(messages);
                }
            }
        }
    }

    private void warpBlock(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex,
                           Iterable<IM> messages,
                           RangeMap<T, S> collectedStates) throws IOException {
        long time = System.nanoTime();
        RangeMap<T, Pair<S, PW>> warppedIntervals =
                intervalVertex.warp(messages, intervalVertex.getValue().getPropertyMap(getPropertyLabelForCompute()));
        time = System.nanoTime() - time;
        localDebugData.WarpDuplicationCount += intervalVertex.messageDuplicationCount;
        localDebugData.WarpRegion += time;
        localDebugData.WarpCalls += 1;

        Collection<Pair<Interval<T>, S>> updatedIntervalStates;
        for (Map.Entry<Range<T>, Pair<S, PW>> warpInterval : warppedIntervals.asMapOfRanges().entrySet()) {
            /** User-Controlled Filter decides if compute should be invoked*/
            if(!filterWarpOutput(warpInterval.getKey(), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight())) {
                time = System.nanoTime();
                updatedIntervalStates = compute(intervalVertex, intervalVertex.createInterval(warpInterval.getKey()), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
                time = System.nanoTime() - time;
                localDebugData.CRegion += time;
                localDebugData.CompCalls += 1;

                for(Pair<Interval<T>, S> updatedIntervalState : updatedIntervalStates) {
                    collectedStates.putCoalescing(Range.closedOpen(updatedIntervalState.getKey().getStart(), updatedIntervalState.getKey().getEnd()),
                            updatedIntervalState.getValue());
                }
                if(updatedIntervalStates.isEmpty())
                    localDebugData.RCompCalls += 1;
            }
        }
    }
}
