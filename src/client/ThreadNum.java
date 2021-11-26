import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ThreadNum {

    public static final ThreadNum INSTANCE = new ThreadNum();

    private Set<String> threadSet = Collections.newSetFromMap(new ConcurrentHashMap());


    public static ThreadNum getInstance() {
        return INSTANCE;
    }

    public Set<String> getThreadSet() {
        return threadSet;
    }

    public void setThreadSet(Set<String> threadSet) {
        this.threadSet = threadSet;
    }

    public void addThreadSet(String threadName) {
        this.threadSet.add(threadName);
    }

    public void removeThreadSet(String threadName) {
        this.threadSet.remove(threadName);
    }
}
