package io.pravega.leaderelection;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.leaderelection.impl.LeaderElectionImpl;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class LeaderElection {

    private final LeaderElectionImpl leaderElectionImpl;
    private final ClientFactory clientFactory;
    private final StreamManager streamManager;
    private final String scopeName;
    private final String streamName;

    public LeaderElection(String scope, String configName, URI controllerURI, Callback listener, String hostName) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(configName);
        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(listener);
        this.scopeName = scope;
        this.streamName = configName;
        this.clientFactory = ClientFactory.withScope(scope, controllerURI);
        this.streamManager = StreamManager.create(controllerURI);
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        leaderElectionImpl = new LeaderElectionImpl(scope, configName, clientFactory, streamManager, pool, listener, hostName);

    }

    public void create() {
         leaderElectionImpl.add();
         leaderElectionImpl.startAsync();

    }

    public void stop() {
        leaderElectionImpl.stopAsync();
    }

    public interface Callback {
        void selectLeader(String name);
        void crashLeader(String name);

    }

    public void deleteStream() {
        try {
            streamManager.sealStream(scopeName, streamName);
            Thread.sleep(500);
            streamManager.deleteStream(scopeName,streamName);
            Thread.sleep(500);
        } catch (InterruptedException e) {
            log.error("Problem while sleeping current Thread in deleteStreams: {}.", e);
        }
    }

    public void close() {
        if (leaderElectionImpl != null) {
            leaderElectionImpl.close();
        }
        if (streamManager != null) {
            streamManager.close();
        }
        if (clientFactory != null) {
            clientFactory.close();
        }
    }

    public Set<String> getCurrentMembers() {
        return leaderElectionImpl.getCurrentMembers();
    }

    public String getCurrentLeader() {
        return leaderElectionImpl.getCurrentLeader();
    }

    public String getInstanceId() {
        return leaderElectionImpl.getInstanceId();
    }

}
