package in.dreamlab.wicm.graph.computation;

import org.apache.log4j.Logger;

import java.util.Arrays;

public class PerfDebugData {
    public long RCompCalls; // user compute calls with no effect
    public long CompCalls; // user compute calls
    public long ScattCalls; // user scatter calls
    public long WarpCalls; // warp calls
    public long giraphCompCalls; // giraph compute calls
    public long giraphRCompCalls; // giraph compute calls with no effect

    // Warp specific debug data
    public long WarpDuplicationCount; // number of message duplication
    public double WarpCost; // warp cost incurred: M log M
    public double NewWarpCost; // warp cost incurred: (M+S)log(M+S)
    public long[] WarpMessageDistribution = new long[500]; // message distribution for warp: 1-5, 5-10, ... 4995+

    public long ICRegion; // time spent in interval compute
    public long RedundantICRegion; // time spent for redundant interval compute
    public long CRegion; // time spent in user compute
    public long _SRegion; // time spent in _Scatter
    public long MsgSerRegion; // time spent in message serialisation
    public long SRegion; // time spent in user scatter
    public long InitRegion; // time spent in init
    public long WarpRegion; // time spent in warp
    public long SuperstepRegion; // time spent in superstep

    public long RUpdate; // number of updates: Update is count only if it leads to scatter
    public long Messages; // number of messages

    public void initialise(){
        RCompCalls = 0;
        CompCalls = 0;
        ScattCalls = 0;
        WarpCalls = 0;
        giraphCompCalls = 0;
        giraphRCompCalls = 0;

        WarpCost = 0;
        NewWarpCost = 0;
        WarpDuplicationCount = 0;
        Arrays.fill(WarpMessageDistribution, 0L);

        ICRegion = 0;
        RedundantICRegion = 0;
        InitRegion = 0;
        WarpRegion = 0;
        CRegion = 0;
        _SRegion = 0;
        SRegion = 0;
        MsgSerRegion = 0;
        SuperstepRegion = 0;

        RUpdate = 0;
        Messages = 0;
    }

    public void add(PerfDebugData other){
        RCompCalls += other.RCompCalls;
        CompCalls += other.CompCalls;
        ScattCalls += other.ScattCalls;
        WarpCalls += other.WarpCalls;
        giraphCompCalls += other.giraphCompCalls;
        giraphRCompCalls += other.RCompCalls;

        WarpCost += other.WarpCost;
        NewWarpCost += other.NewWarpCost;
        WarpDuplicationCount += other.WarpDuplicationCount;
        for(int i=0; i<WarpMessageDistribution.length; i++) WarpMessageDistribution[i] += other.WarpMessageDistribution[i];

        ICRegion += other.ICRegion;
        RedundantICRegion += other.RedundantICRegion;
        InitRegion += other.InitRegion;
        WarpRegion += other.WarpRegion;
        CRegion += other.CRegion;
        _SRegion += other._SRegion;
        SRegion += other.SRegion;
        MsgSerRegion += other.MsgSerRegion;

        RUpdate += other.RUpdate;
        Messages += other.Messages;
    }

    public void dump(Logger LOG, long superstep) {
        LOG.info(superstep+",RedundantComputeCalls,"+RCompCalls);
        LOG.info(superstep+",TotalComputeCalls,"+CompCalls);
        LOG.info(superstep+",TotalScatterCalls,"+ScattCalls);
        LOG.info(superstep+",TotalWarpCalls,"+WarpCalls);
        LOG.info(superstep+",GiraphComputeCalls,"+giraphCompCalls);
        LOG.info(superstep+",GiraphRedundantComputeCalls,"+giraphRCompCalls);

        LOG.info(superstep+",TotalWarpCost,"+WarpCost);
        LOG.info(superstep+",TotalNewWarpCost,"+NewWarpCost);
        LOG.info(superstep+",TotalWarpDuplicationCount,"+WarpDuplicationCount);
        LOG.info(superstep+",WarpMessageDistribution,"+Arrays.toString(WarpMessageDistribution));

        LOG.info(superstep+",IntervalComputeTime,"+ICRegion);
        LOG.info(superstep+",RedundantIntervalComputeTime,"+RedundantICRegion);
        LOG.info(superstep+",InitRegion,"+InitRegion);
        LOG.info(superstep+",WarpRegion,"+WarpRegion);
        LOG.info(superstep+",UserComputeTime,"+CRegion);
        LOG.info(superstep+",_ScatterTime,"+_SRegion);
        LOG.info(superstep+",MessageSerialisationTime,"+MsgSerRegion);
        LOG.info(superstep+",UserScatterTime,"+SRegion);

        LOG.info(superstep+",ReUpdates,"+RUpdate);
        LOG.info(superstep+",Messages,"+Messages);
    }
}
