package eu.ahref;

import sun.misc.Signal;

import java.util.Observable;

/**
 * Created by IntelliJ IDEA.
 * User: martino
 * Date: 1/28/11
 * Time: 9:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class SignalHandler extends Observable implements sun.misc.SignalHandler{
    public void handle(Signal signal) {
        setChanged();
        notifyObservers(signal);
    }

    public void handleSignal(String signame) throws IllegalArgumentException{
        try{
            sun.misc.Signal.handle(new sun.misc.Signal(signame), this);
        }catch(IllegalArgumentException e){
            throw e;
        }catch(Throwable x){
            throw new IllegalArgumentException("Signal unsupported: "+signame, x);
        }
    }

}
