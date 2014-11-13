package ca.uwo.eng.sel.cepsim.network;

import ca.uwo.eng.sel.cepsim.query.Vertex;

/**
 * Created by virso on 2014-11-10.
 */
public interface NetworkInterface {

    public void sendMessage(double timestamp, Vertex orig, Vertex dest, int quantity);

}
