/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.leaderelection;

/**
 * The interface of LeaderElection callback.
 */
public interface LeaderElectionCallback {
    /**
     * When leader changes, every host will call this function.
     * @param name The host which become leader.
     */
    void onNewLeader(String name);

    /**
     * When leader is unhealthy and become healthy,
     * it will call this function to act as leader.
     */
    void startActingLeader();

    /**
     * When leader is healthy and become unhealthy,
     * it will call this function to stop acting as leader.
     */
    void stopActingLeader();

}
