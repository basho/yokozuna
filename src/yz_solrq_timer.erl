-module(yz_solrq_timer).
-export([send_after/3]).

send_after(DelayMS, Pid, Msg) -> % broken out for eqc_mocking
    erlang:send_after(DelayMS, Pid, Msg).

