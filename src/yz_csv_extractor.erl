%% @doc This module provides an extractor for CSV data.  This
%% extractor supports plain text files in a format similar to RFC-4180
%% [http://tools.ietf.org/html/rfc4180].  It favors speed over strict
%% adherence to the RFC.  Following are requirements for a given value
%% to be successfully parsed.
%%
%% 1. By default the field separator is a comma (0x2C) but an
%% alternate separator may be specified as any byte besides newline
%% (0x0A) or carriage return (0x0D).  E.g. a TAB (0x09) may be used.
%%
%% 2. The first line must be a header.  The header column names must
%% match field names defined in the schema.
%%
%% 3. The line-ending sequence is assumed to be Unix (0X0A) or Windows
%% style (0x0D 0x0A) and will be automitically determined from the
%% header line.  Every line must use the same line-ending style.
%%
%% 4. Each record maps to a solr document.  An extra doc is written to
%% keep track of the hash that represents all the records.  Thus, if a
%% CSV object contains 50 records 51 docs would be written.
%%
%% 5. If a field is "null" then the corresponding doc will not have an
%% entry for that field.
%%
%% TODO: CSV implies creating many solr docs from one KV object.  This
%% poses a problem with AAE because it expects a one-to-one mapping
%% between KV objects and solr docs.  However, each CSV record could
%% create it's own solr doc _without_ the `_yz_ed' field and instead a
%% single doc could be written with just the `_yz_ed' field along with
%% the docs representing each record.  The single doc would represent
%% the hash for the collection of docs created by all the records of
%% CSV object.
%%
%% Actually, the one thing I'm forgetting here is the `id' field.
%% That needs to be unique for each record.  One option is to assign
%% an number for each record and append that to the `id' field.  This
%% would make the value `<riak key>_<logical partition num>_<record
%% num>`.  Another option is to have user pass in a field name which
%% represents a unique id for each record.  The problem with this is
%% it's a) more work for the suer and b) is susceptible to "null"
%% field values.
-module(yz_csv_extractor).
