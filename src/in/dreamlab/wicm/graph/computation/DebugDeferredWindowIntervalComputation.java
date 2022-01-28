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

/**
 * This class is used for building algorithms on WICM + LU + DS.
 * LU is enabled if ENABLE_BLOCK parameter is set to True.
 * @param <I> Vertex ID data type
 * @param <T> Time data type
 * @param <S> Vertex state data type
 * @param <V> Vertex Interval State
 * @param <EP> Edge Property data type
 * @param <E> Edge Interval State
 * @param <PW> Warpped Message data type
 * @param <P> Message payload data type
 * @param <IM> Message class
 */
public abstract class DebugDeferredWindowIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends BasicIntervalComputation<I, T, S, V, EP, E, PW, P, IM> implements WICMConstants {
    protected static Logger LOG = Logger.getLogger(DebugDeferredWindowIntervalComputation.class);

    private final BooleanConfOption dumpPerfData = new BooleanConfOption("debugPerformance", false, "Collect debug metrics");

    protected GraphiteDebugWindowWorkerContext worker;
    protected boolean isInitial; // first superstep for a window?
    protected Interval<T> windowInterval; // current window interval
    protected Interval<T> spareInterval; // unprocessed future interval

    private RangeMap<T, S> changedStates; // maintain collected states
    private LocalWritableMessageBuffer<IM> messageBuffer;

    private boolean isUseful;
    private PerfDebugData localDebugData = new PerfDebugData();

    @Override
    public void preSuperstep() {
        if(WICMConstants.ENABLE_BLOCK.get(getConf())) { // local warp unrolling is enabled
            LOG.info("Creating buffer with size " + BUFFER_SIZE.get(getConf()) + " and min messages " + MIN_MESSAGES.get(getConf()));
            messageBuffer = new LocalWritableMessageBuffer<>(BUFFER_SIZE.get(getConf()), getConf().getOutgoingMessageValueClass());
        }
        changedStates = TreeRangeMap.create();
        worker = this.getWorkerContext();
        localDebugData.initialise();
    }

    @Override
    public void postSuperstep() {
        if(dumpPerfData.get(getConf())){
            localDebugData.dump(LOG, getSuperstep());
        }

        // no messages sent by this compute thread, worker possibly finished?
        worker.finished.set(worker.finished.get() && (localDebugData.Messages == 0));
    }

    public abstract boolean isDefault(S vertexValue);

    @Override
    public void compute(Vertex<I, V, E> vertex, Iterable<IM> messages) throws IOException {
        long intervalComputeRegion = System.nanoTime();
        intervalCompute((IntervalVertex<I, T, S, V, EP, E, PW, P, IM>) vertex, messages);
        intervalComputeRegion = System.nanoTime() - intervalComputeRegion;

        if(!isUseful){ // this giraph compute was a no-op call
            localDebugData.RedundantICRegion += intervalComputeRegion;
            localDebugData.giraphRCompCalls += 1;
        }
        localDebugData.ICRegion += intervalComputeRegion;
        localDebugData.giraphCompCalls += 1;
    }

    public void intervalCompute(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Iterable<IM> messages)
            throws IOException {
        long timedRegion;

        if (isInitial || (getSuperstep() == 0)) { // first superstep of any window
            boolean doScatter = true;
            isUseful = false;
            if (getSuperstep() == 0) { // initialise vertex states if first superstep of application
                isUseful = true;
                timedRegion = System.nanoTime();
                doScatter = init(intervalVertex);
                timedRegion = System.nanoTime() - timedRegion;
                localDebugData.InitRegion += timedRegion;
            }

            // Initialise execution by calling scatter and sending messages.
            if(doScatter) {
                for (Map.Entry<Range<T>, S> vertexState : intervalVertex.getState(windowInterval)) {
                    if(!isDefault(vertexState.getValue())) { // propagate only non-default values
                        isUseful = true;
                        timedRegion = System.nanoTime();
                        _scatter(intervalVertex, intervalVertex.createInterval(vertexState.getKey()), vertexState.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData._SRegion += timedRegion;
                    }
                }

                // if vertex has some future state that should be propagated, vertex will not halt
                intervalVertex.resetVoteToRemainActive();
                for(Map.Entry<Range<T>, S> vertexState : intervalVertex.getState(spareInterval)){
                    if(!isDefault(vertexState.getValue()))
                        intervalVertex.voteToRemainActive();
                }
            }
        } else {
            isUseful = true;
            if(Iterables.isEmpty(messages)) { // return if no messages => no-op call!
                isUseful = false;
                return;
            }

            changedStates.clear();
            int messageSize = Iterables.size(messages);
            int index = Integer.min((messageSize-1)/10, 499);
            localDebugData.WarpMessageDistribution[index] += 1;

            if(WICMConstants.ENABLE_BLOCK.get(getConf())) { // process messages in blocks
                messageBuffer.reset(); // clear cache

                // get number of message sets with uniform set size
                int numBlocks = 1, blockSize = messageSize;
                if (messageSize >= MIN_MESSAGES.get(getConf())) {
                    numBlocks = (int) Math.floor(Math.sqrt(messageSize));
                    blockSize = Integer.min(BUFFER_SIZE.get(getConf()), ((int) Math.ceil(messageSize * 1. / numBlocks)));
                }

                for(IM m : messages) {
                    messageBuffer.addMessage(m); // fill cache with deserialised messages
                    if(messageBuffer.filledBufferSize() == blockSize) {  // message buffer full, apply warp
                        localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                        warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
                        messageBuffer.reset(); // clear cache
                    }
                }

                if(!messageBuffer.isEmpty()) { // last message set
                    localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                    warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
                }
            } else { // single time warp
                localDebugData.WarpCost += (messageSize*Math.log(messageSize+0.1));

                /** Compute-Side Warp*/
                timedRegion = System.nanoTime();
                RangeMap<T, Pair<S, PW>> warppedIntervals = intervalVertex.warp(messages, intervalVertex.getValue().getPropertyMap(getPropertyLabelForCompute()));
                timedRegion = System.nanoTime() - timedRegion;
                localDebugData.WarpDuplicationCount += intervalVertex.messageDuplicationCount;
                localDebugData.WarpRegion += timedRegion;
                localDebugData.WarpCalls += 1;

                Collection<Pair<Interval<T>, S>> updatedIntervalStates;
                for (Map.Entry<Range<T>, Pair<S, PW>> warpInterval : warppedIntervals.asMapOfRanges().entrySet()) {
                    // User-Controlled Filter decides if compute should be invoked
                    // By default, filterWarpOutput is always False
                    if(!filterWarpOutput(warpInterval.getKey(), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight())) {
                        // call user-defined compute operation
                        timedRegion = System.nanoTime();
                        updatedIntervalStates = compute(intervalVertex, intervalVertex.createInterval(warpInterval.getKey()), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.CRegion += timedRegion;
                        localDebugData.CompCalls += 1;

                        // collect and coalesce updated states
                        for(Pair<Interval<T>, S> updatedIntervalState : updatedIntervalStates) {
                            changedStates.putCoalescing(Range.closedOpen(updatedIntervalState.getKey().getStart(), updatedIntervalState.getKey().getEnd()), updatedIntervalState.getValue());
                            // state in some future window, dont halt
                            if(spareInterval.intersects(updatedIntervalState.getKey()))
                                intervalVertex.voteToRemainActive();
                        }

                        if(updatedIntervalStates.isEmpty())
                            localDebugData.RCompCalls += 1;
                    }
                }
            }

            // for collected states, run scatter
            for(Map.Entry<Range<T>, S> updatedIntervalState : changedStates.asMapOfRanges().entrySet()) {
                Interval<T> stateInterval = intervalVertex.createInterval(updatedIntervalState.getKey());
                if(windowInterval.intersects(updatedIntervalState.getKey())){ // state in current window, need to propagate information via scatter
                    Interval<T> intersectionInterval = stateInterval.getIntersection(windowInterval);
                    timedRegion = System.nanoTime();
                    _scatter(intervalVertex, intersectionInterval, updatedIntervalState.getValue());
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData._SRegion += timedRegion;
                }
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
                if(getPropertyLabelForScatter()!=null) { // edge can have temporal properties
                    // State can span multiple fragemented sub-intervals of a single edge
                    for(Map.Entry<Range<T>, EP> edgeSubInterval : edge.getValue().getProperty(getPropertyLabelForScatter(), intersection)) {
                        // Call user-defined scatter operation
                        timedRegion = System.nanoTime();
                        Iterable<IM> messages = scatter(intervalVertex, edge, (Interval<T>) intervalVertex.createInterval(edgeSubInterval.getKey()), vState, edgeSubInterval.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.SRegion += timedRegion;
                        localDebugData.ScattCalls += 1;

                        // send messages generated
                        timedRegion = System.nanoTime();
                        for(IM msg : messages) {
                            sendMessage(edge.getTargetVertexId(), msg);
                        }
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.MsgSerRegion += timedRegion;
                        localDebugData.Messages += Iterables.size(messages);
                    }
                } else { // in our case, edges do not have temporal properties
                    timedRegion = System.nanoTime();
                    Iterable<IM> messages = scatter(intervalVertex, edge, intersection, vState, null);
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.SRegion += timedRegion;
                    localDebugData.ScattCalls += 1;

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
        /** Compute-Side Warp*/
        long time = System.nanoTime();
        RangeMap<T, Pair<S, PW>> warppedIntervals =
                intervalVertex.warp(messages, intervalVertex.getValue().getPropertyMap(getPropertyLabelForCompute()));
        time = System.nanoTime() - time;
        localDebugData.WarpDuplicationCount += intervalVertex.messageDuplicationCount;
        localDebugData.WarpRegion += time;
        localDebugData.WarpCalls += 1;

        Collection<Pair<Interval<T>, S>> updatedIntervalStates;
        for (Map.Entry<Range<T>, Pair<S, PW>> warpInterval : warppedIntervals.asMapOfRanges().entrySet()) {
            if(!filterWarpOutput(warpInterval.getKey(), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight())) {
                time = System.nanoTime();
                updatedIntervalStates = compute(intervalVertex, intervalVertex.createInterval(warpInterval.getKey()), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
                time = System.nanoTime() - time;
                localDebugData.CRegion += time;
                localDebugData.CompCalls += 1;

                for(Pair<Interval<T>, S> updatedIntervalState : updatedIntervalStates) {
                    collectedStates.putCoalescing(Range.closedOpen(updatedIntervalState.getKey().getStart(), updatedIntervalState.getKey().getEnd()),
                            updatedIntervalState.getValue());
                    // state in some future window, dont halt
                    if(spareInterval.intersects(updatedIntervalState.getKey()))
                        intervalVertex.voteToRemainActive();
                }
                if(updatedIntervalStates.isEmpty())
                    localDebugData.RCompCalls += 1;
            }
        }
    }
}
