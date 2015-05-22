%% -*- encoding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012,2013,2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(yz_json_extractor_tests).
-compile(export_all).
-include("yz_test.hrl").

json_extract_test() ->
    {ok, TestJSON} = file:read_file("../test/test.json"),
    Result = yz_json_extractor:extract(TestJSON),
    Expect =
        [{<<"name"/utf8>>,<<"ryan"/utf8>>},
         {<<"age"/utf8>>,<<"29"/utf8>>},
         {<<"pets"/utf8>>,<<"smokey"/utf8>>},
         {<<"pets"/utf8>>,<<"bandit"/utf8>>},
         {<<"books.title"/utf8>>,<<"Introduction to Information Retrieval"/utf8>>},
         {<<"books.title"/utf8>>,<<"Principles of Distributed Database Systems"/utf8>>},
         {<<"books.authors"/utf8>>,<<"Christopher D. Manning"/utf8>>},
         {<<"books.authors"/utf8>>,<<"Prabhakar Raghavan"/utf8>>},
         {<<"books.authors"/utf8>>,<<"Hinrich Schütze"/utf8>>},
         {<<"books.authors"/utf8>>,<<"M. Tamer Özsu"/utf8>>},
         {<<"books.authors"/utf8>>,<<"Patrick Valduriez"/utf8>>},
         {<<"alive"/utf8>>,true},
         {<<"married"/utf8>>,false},
         {<<"a_number"/utf8>>,<<"1100000.0"/utf8>>},
         {<<"lucky_numbers"/utf8>>,<<"13"/utf8>>},
         {<<"lucky_numbers"/utf8>>,<<"17"/utf8>>},
         {<<"lucky_numbers"/utf8>>,<<"21"/utf8>>}],

    %% Do one at a time so failure is easier to understand
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:sort(Expect), lists:sort(Result)),
    [?assertEqual(E,R) || {E,R} <- Pairs],
    %% Verify conversion doesn't error
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).

utf8_test() ->
    {ok, JSON} = file:read_file("../test/utf8.json"),
    Result = yz_json_extractor:extract(JSON),
    case Result of
        {error, Reason} ->
            ?debugFmt("~nextract/1 failed: ~s~n", [Reason]),
            throw(extract_failed);
        _ ->
            ok
    end,
    Expect =
        [{<<"langs.english"/utf8>>, <<"The quick brown fox jumps over the lazy dog."/utf8>>},
         {<<"langs.jamaican"/utf8>>, <<"Chruu, a kwik di kwik brong fox a jomp huova di liezi daag de, yu no siit?"/utf8>>},
         {<<"langs.irish"/utf8>>, <<"\"An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí do leasa ṫú?\" \"D'ḟuascail Íosa Úrṁac na hÓiġe Beannaiṫe pór Éava agus Áḋaiṁ.\""/utf8>>},
         {<<"langs.dutch"/utf8>>, <<"Pa's wĳze lynx bezag vroom het fikse aquaduct."/utf8>>},
         {<<"langs.german_1"/utf8>>, <<"Falsches Üben von Xylophonmusik quält jeden größeren Zwerg."/utf8>>},
         {<<"langs.german_2"/utf8>>, <<"Im finſteren Jagdſchloß am offenen Felsquellwaſſer patzte der affig-flatterhafte kauzig-höf‌liche Bäcker über ſeinem verſifften kniffligen C-Xylophon."/utf8>>},
         {<<"langs.norwegian"/utf8>>, <<"Blåbærsyltetøy."/utf8>>},
         {<<"langs.danish"/utf8>>, <<"Høj bly gom vandt fræk sexquiz på wc."/utf8>>},
         {<<"langs.swedish"/utf8>>, <<"Flygande bäckasiner söka strax hwila på mjuka tuvor."/utf8>>},
         {<<"langs.icelandic"/utf8>>, <<"Sævör grét áðan því úlpan var ónýt."/utf8>>},
         {<<"langs.finnish"/utf8>>, <<"Törkylempijävongahdus."/utf8>>},
         {<<"langs.polish"/utf8>>, <<"Pchnąć w tę łódź jeża lub osiem skrzyń fig."/utf8>>},
         {<<"langs.czech"/utf8>>, <<"Příliš žluťoučký kůň úpěl ďábelské kódy."/utf8>>},
         {<<"langs.slovak"/utf8>>, <<"Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote."/utf8>>},
         {<<"langs.greek_monotonic"/utf8>>, <<"ξεσκεπάζω την ψυχοφθόρα βδελυγμία"/utf8>>},
         {<<"langs.greek_polytonic"/utf8>>, <<"ξεσκεπάζω τὴν ψυχοφθόρα βδελυγμία"/utf8>>},
         {<<"langs.russian"/utf8>>, <<"Съешь же ещё этих мягких французских булок да выпей чаю."/utf8>>},
         {<<"langs.bulgarian"/utf8>>, <<"Жълтата дюля беше щастлива, че пухът, който цъфна, замръзна като гьон."/utf8>>},
         {<<"langs.sami"/utf8>>, <<"Vuol Ruoŧa geđggiid leat máŋga luosa ja čuovžža."/utf8>>},
         {<<"langs.hungarian"/utf8>>, <<"Árvíztűrő tükörfúrógép."/utf8>>},
         {<<"langs.spanish"/utf8>>, <<"El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro."/utf8>>},
         {<<"langs.portuguese"/utf8>>, <<"O próximo vôo à noite sobre o Atlântico, põe freqüentemente o único médico."/utf8>>},
         {<<"langs.french"/utf8>>, <<"Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être déçus en voyant leurs drôles d'œufs abîmés."/utf8>>},
         {<<"langs.esperanto"/utf8>>, <<"Eĥoŝanĝo ĉiuĵaŭde."/utf8>>},
         {<<"langs.hebrew"/utf8>>, <<"זה כיף סתם לשמוע איך תנצח קרפד עץ טוב בגן."/utf8>>},
         {<<"langs.japanese_hiragana"/utf8>>, <<"
    いろはにほへど　ちりぬるを
    わがよたれぞ　つねならむ
    うゐのおくやま　けふこえて
    あさきゆめみじ　ゑひもせず
  "/utf8>>},
        {<<"langs.japanese_kanji"/utf8>>, <<"
    色は匂へど 散りぬるを
    我が世誰ぞ 常ならむ
    有為の奥山 今日越えて
    浅き夢見じ 酔ひもせず
  "/utf8>>},
         {<<"langs.английский"/utf8>>, <<"The quick brown fox jumps over the lazy dog."/utf8>>},
         {<<"langs.chinese"/utf8>>, <<"
    花非花
    雾非雾
    夜半来
    天明去
    来如春梦几多时
    去似朝云无觅处
  "/utf8>>}],
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:keysort(1, Expect), lists:keysort(1, Result)),
    [?assertPairsEq(E,R) || {E,R} <- Pairs],
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
