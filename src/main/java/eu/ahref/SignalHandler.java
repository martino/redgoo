package eu.ahref;

import sun.misc.Signal;

import java.util.Observable;

/**
 * Observable class that implements SUN SignalHandler
 * @author Martino Pizzol
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
