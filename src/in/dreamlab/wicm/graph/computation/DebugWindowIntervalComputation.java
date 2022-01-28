package in.dreamlab.wicm.graph.computation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graph.computation.BasicIntervalComputation;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.graphite.types.Interval;
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
 * This class is used for building algorithms on WICM.
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
public abstract class DebugWindowIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends BasicIntervalComputation<I, T, S, V, EP, E, PW, P, IM> {
    protected static Logger LOG = Logger.getLogger(DebugWindowIntervalComputation.class);

    private final BooleanConfOption dumpPerfData = new BooleanConfOption("debugPerformance", false, "Collect debug metrics");

    protected GraphiteDebugWindowWorkerContext worker;
    protected boolean isInitial; // first superstep for a window?
    protected Interval<T> windowInterval; // current window interval
    protected Interval<T> spareInterval; // unprocessed future interval

    private boolean isUseful;
    private PerfDebugData localDebugData = new PerfDebugData(); // store important log information

    @Override
    public void preSuperstep() {
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

    /** */
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

            int messageSize = Iterables.size(messages);
            int index = Integer.min((messageSize-1)/10, 499);
            localDebugData.WarpMessageDistribution[index] += 1;
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

                    // for updated states, call scatter
                    for(Pair<Interval<T>, S> updatedIntervalState : updatedIntervalStates) {
                        // state in some future window, vertex will not halt
                        if(spareInterval.intersects(updatedIntervalState.getKey()))
                            intervalVertex.voteToRemainActive();

                        // state in current window, need to propagate information via scatter
                        if(windowInterval.intersects(updatedIntervalState.getKey())){
                            Interval<T> intersectionInterval = updatedIntervalState.getKey().getIntersection(windowInterval);

                            timedRegion = System.nanoTime();
                            _scatter(intervalVertex, intersectionInterval, updatedIntervalState.getValue());
                            timedRegion = System.nanoTime() - timedRegion;
                            localDebugData._SRegion += timedRegion;
                        }
                    }

                    if(updatedIntervalStates.isEmpty())
                        localDebugData.RCompCalls += 1;
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
                    // Call user-defined scatter operation
                    timedRegion = System.nanoTime();
                    Iterable<IM> messages = scatter(intervalVertex, edge, intersection, vState, null);
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
            }
        }
    }
}
