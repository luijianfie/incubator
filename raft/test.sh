go test -timeout 30s -run ^TestProgressLeader2AB$
go test -timeout 30s -run ^TestLeaderElection2AA$
go test -timeout 30s -run ^TestLeaderCycle2AA$
go test -timeout 30s -run ^TestLeaderElectionOverwriteNewerLogs2AB$
go test -timeout 30s -run ^TestVoteFromAnyState2AA$
go test -timeout 30s -run ^TestLogReplication2AB$
go test -timeout 30s -run ^TestSingleNodeCommit2AB$
go test -timeout 30s -run ^TestCommitWithoutNewTermEntry2AB$
go test -timeout 30s -run ^TestCommitWithHeartbeat2AB$
go test -timeout 30s -run ^TestDuelingCandidates2AB$
go test -timeout 30s -run ^TestCandidateConcede2AB$
go test -timeout 30s -run ^TestSingleNodeCandidate2AA$
go test -timeout 30s -run ^TestOldMessages2AB$
go test -timeout 30s -run ^TestProposal2AB$
go test -timeout 30s -run ^TestHandleMessageType_MsgAppend2AB$
go test -timeout 30s -run ^TestRecvMessageType_MsgRequestVote2AA$
go test -timeout 30s -run ^TestAllServerStepdown2AB$
go test -timeout 30s -run ^TestCandidateResetTermMessageType_MsgHeartbeat2AA$
go test -timeout 30s -run ^TestCandidateResetTermMessageType_MsgAppend2AA$
go test -timeout 30s -run ^TestDisruptiveFollower2AA$
go test -timeout 30s -run ^TestHeartbeatUpdateCommit2AB$
go test -timeout 30s -run ^TestRecvMessageType_MsgBeat2AA$
go test -timeout 30s -run ^TestLeaderIncreaseNext2AB$
go test -timeout 30s -run ^TestRestoreSnapshot2C$
go test -timeout 30s -run ^TestRestoreIgnoreSnapshot2C$
go test -timeout 30s -run ^TestProvideSnap2C$
go test -timeout 30s -run ^TestRestoreFromSnapMsg2C$
go test -timeout 30s -run ^TestSlowNodeRestore2C$
go test -timeout 30s -run ^TestAddNode3A$
go test -timeout 30s -run ^TestRemoveNode3A$
go test -timeout 30s -run ^TestCampaignWhileLeader2AA$
go test -timeout 30s -run ^TestCommitAfterRemoveNode3A$
go test -timeout 30s -run ^TestLeaderTransferToUpToDateNode3A$
go test -timeout 30s -run ^TestLeaderTransferToUpToDateNodeFromFollower3A$
go test -timeout 30s -run ^TestLeaderTransferToSlowFollower3A$
go test -timeout 30s -run ^TestLeaderTransferAfterSnapshot3A$
go test -timeout 30s -run ^TestLeaderTransferToSelf3A$
go test -timeout 30s -run ^TestLeaderTransferToNonExistingNode3A$
go test -timeout 30s -run ^TestLeaderTransferReceiveHigherTermVote3A$
go test -timeout 30s -run ^TestLeaderTransferRemoveNode3A$
go test -timeout 30s -run ^TestLeaderTransferBack3A$
go test -timeout 30s -run ^TestLeaderTransferSecondTransferToAnotherNode3A$
go test -timeout 30s -run ^TestTransferNonMember3A$
go test -timeout 30s -run ^TestSplitVote2AA$
go test -timeout 30s -run ^newTestConfig$
go test -timeout 30s -run ^newTestRaft$