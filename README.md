# pravega-leaderElection
This is the external LeaderElection API based on pravega state synchronizer.
## Pre-requisties
Pravega running.  
Install the Lombok Plugin. This can be found in Preferences -> Plugins. Restart your IDE
## Running test
The scpoename, streamname uri and hostname are configurable.  
if not included, the defaults are:  
scope - "test"  
name - "leaderElection"  
uri - "tcp://127.0.0.1:9090"  
hostname - "host-i" (i from 0 to the size of the group)  
### Testcase
There are 7 testcase in leaderElection test.
1. testInitialElection3.
2. testMemberCrash3.
3. testReElection3.
4. testNoLeader3.
5. testLeaderCorrectness3.
6. testAllCrash3.
7. testReElection5.

For running single test.
run:
gradle test --tests \*LeaderElectionTest.testInitialElection3

For running all tests. You need to delete stream between each test.
