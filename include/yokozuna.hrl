%%%===================================================================
%%% Records
%%%===================================================================

-record(solr_vclocks, {
          more=false :: boolean(),
          continuation :: base64() | none,
          pairs :: [{DocID::binary(), VClock::base64()}]
         }).

%%%===================================================================
%%% Types
%%%===================================================================

-type name() :: atom().
-type value() :: term().
-type field() :: {name(), value()}.
-type fields() :: [field()].
-type doc() :: {doc, fields()}.
-type base64() :: base64:ascii_string().
-type solr_vclocks() :: #solr_vclocks{}.
-type iso8601() :: string().

