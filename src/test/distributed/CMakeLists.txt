add_shadow_tests(
    BASENAME distributed-child-failure
    EXPECT_ERROR TRUE
    ARGS --distributed-shard-count 2 --use-new-tcp true
    PROPERTIES PASS_REGULAR_EXPRESSION "distributed shard ShardId[(][0-9]+[)] exited")
