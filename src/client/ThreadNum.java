import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ThreadNum {

    public static final ThreadNum INSTANCE = new ThreadNum();

    private Map<String, Integer> threadSet = new ConcurrentHashMap<String, Integer>();


    public static ThreadNum getInstance() {
        return INSTANCE;
    }

    public Set<String> getThreadSet() {
        return threadSet.keySet();
    }


    public void addThreadSet(String threadName) {
        this.threadSet.put(threadName, 1);
    }

    public void removeThreadSet(String threadName) {
        this.threadSet.remove(threadName);
    }
}
