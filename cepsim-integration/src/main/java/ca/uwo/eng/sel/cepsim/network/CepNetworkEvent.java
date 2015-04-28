package ca.uwo.eng.sel.cepsim.network;

import ca.uwo.eng.sel.cepsim.metric.EventSet;
import ca.uwo.eng.sel.cepsim.query.InputVertex;
import ca.uwo.eng.sel.cepsim.query.OutputVertex;

/**
 * Created by virso on 2014-11-11.
 */
public class CepNetworkEvent implements Comparable<CepNetworkEvent> {

    private double origTimestamp;
    private double destTimestamp;
    private OutputVertex orig;
    private InputVertex dest;
    private EventSet eventSet;

    public CepNetworkEvent(double origTimestamp, OutputVertex orig, double destTimestamp, InputVertex dest,
                           EventSet eventSet) {
        this.origTimestamp = origTimestamp;
        this.destTimestamp = destTimestamp;
        this.orig = orig;
        this.dest = dest;
        this.eventSet = eventSet;
    }

    public double getOrigTimestamp() {
        return origTimestamp;
    }

    public OutputVertex getOrig() {
        return orig;
    }

    public double getDestTimestamp() {
        return destTimestamp;
    }

    public InputVertex getDest() {
        return dest;
    }

    public EventSet getEventSet() {
        return eventSet;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CepNetworkEvent that = (CepNetworkEvent) o;

        if (Double.compare(that.destTimestamp, destTimestamp) != 0) return false;
        if (Double.compare(that.origTimestamp, origTimestamp) != 0) return false;
        if (!dest.equals(that.dest)) return false;
        if (!orig.equals(that.orig)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(origTimestamp);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(destTimestamp);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + orig.hashCode();
        result = 31 * result + dest.hashCode();
        return result;
    }

    @Override
    public int compareTo(CepNetworkEvent o) {
        int ret = Double.compare(this.destTimestamp, o.destTimestamp);
        if (ret != 0) return ret;

        ret =  Double.compare(this.origTimestamp, o.origTimestamp);
        if (ret != 0) return ret;

        ret = this.orig.compare(o.orig);
        if (ret != 0) return ret;

        return this.dest.compare(o.dest);
    }
}
