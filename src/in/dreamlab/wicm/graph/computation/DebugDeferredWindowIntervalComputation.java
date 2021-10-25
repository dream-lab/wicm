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
 * We mark vertex active if some state present in future to update
 */
public abstract class DebugDeferredWindowIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends BasicIntervalComputation<I, T, S, V, EP, E, PW, P, IM> implements WICMConstants {
    protected static Logger LOG = Logger.getLogger(DebugDeferredWindowIntervalComputation.class);

    private final BooleanConfOption dumpPerfData = new BooleanConfOption("debugPerformance", false, "Collect debug metrics");

    protected GraphiteDebugWindowWorkerContext worker;
    protected boolean isInitial;
    protected Interval<T> windowInterval; // current window interval
    protected Interval<T> spareInterval; // future interval

    private RangeMap<T, S> changedStates;
    private LocalWritableMessageBuffer<IM> messageBuffer;

    private boolean isUseful;
    private PerfDebugData localDebugData = new PerfDebugData();

    @Override
    public void preSuperstep() {
        if(WICMConstants.ENABLE_BLOCK.get(getConf())) {
            LOG.info("Creating buffer with size " + BUFFER_SIZE.get(getConf()) + " and min messages " + MIN_MESSAGES.get(getConf()));
            messageBuffer = new LocalWritableMessageBuffer<>(BUFFER_SIZE.get(getConf()), getConf().getOutgoingMessageValueClass());
        }
        changedStates = TreeRangeMap.create();
        worker = this.getWorkerContext();
        localDebugData.initialise();
        localDebugData.SuperstepRegion = System.nanoTime();
    }

    @Override
    public void postSuperstep() {
        localDebugData.SuperstepRegion = System.nanoTime() - localDebugData.SuperstepRegion;
        if(dumpPerfData.get(getConf())){
            localDebugData.dump(LOG, getSuperstep());
        }

        worker.finished.set(worker.finished.get() && (localDebugData.Messages == 0));
    }

    public abstract boolean isDefault(S vertexValue);

    @Override
    public void compute(Vertex<I, V, E> vertex, Iterable<IM> messages) throws IOException {
        long intervalComputeRegion = System.nanoTime();
        intervalCompute((IntervalVertex<I, T, S, V, EP, E, PW, P, IM>) vertex, messages);
        intervalComputeRegion = System.nanoTime() - intervalComputeRegion;

        if(!isUseful){
            localDebugData.RedundantICRegion += intervalComputeRegion;
            localDebugData.giraphRCompCalls += 1;
        }
        localDebugData.ICRegion += intervalComputeRegion;
        localDebugData.giraphCompCalls += 1;
    }

    public void intervalCompute(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Iterable<IM> messages)
            throws IOException {
        long timedRegion;

        if (isInitial || (getSuperstep() == 0)) {
            boolean doScatter = true;
            isUseful = false;
            if (getSuperstep() == 0) {
                isUseful = true;
                timedRegion = System.nanoTime();
                doScatter = init(intervalVertex);
                timedRegion = System.nanoTime() - timedRegion;
                localDebugData.InitRegion += timedRegion;
            }

            if(doScatter) {
                for (Map.Entry<Range<T>, S> vertexState : intervalVertex.getState(windowInterval)) {
                    if(!isDefault(vertexState.getValue())) {
                        isUseful = true;
                        timedRegion = System.nanoTime();
                        _scatter(intervalVertex, intervalVertex.createInterval(vertexState.getKey()), vertexState.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData._SRegion += timedRegion;
                    }
                }

                /// if vertex has some future state that should be propagated, vertex will not halt
                intervalVertex.resetVoteToRemainActive();
                for(Map.Entry<Range<T>, S> vertexState : intervalVertex.getState(spareInterval)){
                    if(!isDefault(vertexState.getValue()))
                        intervalVertex.voteToRemainActive();
                }
            }
        } else {
            isUseful = true;
            if(Iterables.isEmpty(messages)) { // return if no messages
                isUseful = false;
                return;
            }

            changedStates.clear();
            int messageSize = Iterables.size(messages);
            int index = Integer.min((messageSize-1)/10, 499);
            localDebugData.WarpMessageDistribution[index] += 1;

            if(WICMConstants.ENABLE_BLOCK.get(getConf())) {
                messageBuffer.reset();

                // process messages in blocks
                int numBlocks = 1, blockSize = messageSize;
                if (messageSize >= MIN_MESSAGES.get(getConf())) {
                    numBlocks = (int) Math.floor(Math.sqrt(messageSize));
                    blockSize = Integer.min(BUFFER_SIZE.get(getConf()), ((int) Math.ceil(messageSize * 1. / numBlocks)));
                }

                for(IM m : messages) {
                    messageBuffer.addMessage(m);
                    if(messageBuffer.filledBufferSize() == blockSize) {
                        localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                        warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
                        messageBuffer.reset();
                    }
                }
                if(!messageBuffer.isEmpty()) {
                    localDebugData.WarpCost += (blockSize*Math.log(blockSize+0.1));
                    warpBlock(intervalVertex, messageBuffer.getIterable(), changedStates);
                }
            } else {
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
                    /** User-Controlled Filter decides if compute should be invoked*/
                    if(!filterWarpOutput(warpInterval.getKey(), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight())) {
                        timedRegion = System.nanoTime();
                        updatedIntervalStates = compute(intervalVertex, intervalVertex.createInterval(warpInterval.getKey()), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.CRegion += timedRegion;
                        localDebugData.CompCalls += 1;

                        /** User can choose to update state only for a sub-interval, this results into interval fragementation */
                        // TODO: object creation overheads present!
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

            /** User can choose to update state only for a sub-interval, this results into interval fragementation */
            for(Map.Entry<Range<T>, S> updatedIntervalState : changedStates.asMapOfRanges().entrySet()) {
                Interval<T> stateInterval = intervalVertex.createInterval(updatedIntervalState.getKey());
                if(windowInterval.intersects(updatedIntervalState.getKey())){
                    Interval<T> intersectionInterval = stateInterval.getIntersection(windowInterval);
                    timedRegion = System.nanoTime();
                    _scatter(intervalVertex, intersectionInterval, updatedIntervalState.getValue());
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData._SRegion += timedRegion;
                    localDebugData.RUpdate += stateInterval.getLength();
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
