%% -*- encoding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2015 Basho Technologies, Inc.
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

-module(yz_xml_extractor_tests).
-compile(export_all).
-include("yz_test.hrl").

make_name_test() ->
    Expect = <<"one.two.three.four">>,
    Stack = ["four", "three", "two", "one"],
    Result = yz_xml_extractor:make_name(<<".">>, Stack),
    ?assertEqual(Expect, Result),

    Expect2 = <<"one_two_three_four_five">>,
    Stack2 = ["five", "four", "three", "two", "one"],
    Result2 = yz_xml_extractor:make_name(<<"_">>, Stack2),
    ?assertEqual(Expect2, Result2).

%% Verify that the XML extractor maintains UTF-8 encoding.
utf8_test() ->
    % raw bytes are ok here, because the XML file has a character set indicator
    {ok, SrcXML} = file:read_file("../test/utf8.xml"),
    Result = yz_xml_extractor:extract(SrcXML),
    case Result of
        {error, Reason} ->
            ?debugFmt("~nextract/1 failed: ~s~n", [Reason]),
            throw(extract_failed);
        _ ->
            ok
    end,
    Expect =
        [{<<"langs.english">>, <<"The quick brown fox jumps over the lazy dog.">>},
         {<<"langs.english@attr">>, <<"The quick">>},
         {<<"langs.jamaican">>, <<"Chruu, a kwik di kwik brong fox a jomp huova di liezi daag de, yu no siit?">>},
         {<<"langs.irish">>, <<"\"An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí do leasa ṫú?\" \"D'ḟuascail Íosa Úrṁac na hÓiġe Beannaiṫe pór Éava agus Áḋaiṁ.\""/utf8>>},
         {<<"langs.dutch">>, <<"Pa's wĳze lynx bezag vroom het fikse aquaduct."/utf8>>},
         {<<"langs.german_1">>, <<"Falsches Üben von Xylophonmusik quält jeden größeren Zwerg."/utf8>>},
         {<<"langs.german_1@attr">>, <<"Falsches Üben"/utf8>>},
         {<<"langs.german_2">>, <<"Im finſteren Jagdſchloß am offenen Felsquellwaſſer patzte der affig-flatterhafte kauzig-höf‌liche Bäcker über ſeinem verſifften kniffligen C-Xylophon."/utf8>>},
         {<<"langs.norwegian">>, <<"Blåbærsyltetøy."/utf8>>},
         {<<"langs.danish">>, <<"Høj bly gom vandt fræk sexquiz på wc."/utf8>>},
         {<<"langs.swedish">>, <<"Flygande bäckasiner söka strax hwila på mjuka tuvor."/utf8>>},
         {<<"langs.icelandic">>, <<"Sævör grét áðan því úlpan var ónýt."/utf8>>},
         {<<"langs.finnish">>, <<"Törkylempijävongahdus."/utf8>>},
         {<<"langs.polish">>, <<"Pchnąć w tę łódź jeża lub osiem skrzyń fig."/utf8>>},
         {<<"langs.czech">>, <<"Příliš žluťoučký kůň úpěl ďábelské kódy."/utf8>>},
         {<<"langs.slovak">>, <<"Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote."/utf8>>},
         {<<"langs.greek_monotonic">>, <<"ξεσκεπάζω την ψυχοφθόρα βδελυγμία"/utf8>>},
         {<<"langs.greek_polytonic">>, <<"ξεσκεπάζω τὴν ψυχοφθόρα βδελυγμία"/utf8>>},
         {<<"langs.russian">>, <<"Съешь же ещё этих мягких французских булок да выпей чаю."/utf8>>},
         {<<"langs.bulgarian"/utf8>>, <<"Жълтата дюля беше щастлива, че пухът, който цъфна, замръзна като гьон."/utf8>>},
         {<<"langs.sami">>, <<"Vuol Ruoŧa geđggiid leat máŋga luosa ja čuovžža."/utf8>>},
         {<<"langs.hungarian">>, <<"Árvíztűrő tükörfúrógép."/utf8>>},
         {<<"langs.spanish">>, <<"El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro."/utf8>>},
         {<<"langs.portuguese">>, <<"O próximo vôo à noite sobre o Atlântico, põe freqüentemente o único médico."/utf8>>},
         {<<"langs.french">>, <<"Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être déçus en voyant leurs drôles d'œufs abîmés."/utf8>>},
         {<<"langs.esperanto">>, <<"Eĥoŝanĝo ĉiuĵaŭde."/utf8>>},
         {<<"langs.hebrew">>, <<"זה כיף סתם לשמוע איך תנצח קרפד עץ טוב בגן."/utf8>>},
         {<<"langs.japanese_hiragana">>, <<"
    いろはにほへど　ちりぬるを
    わがよたれぞ　つねならむ
    うゐのおくやま　けふこえて
    あさきゆめみじ　ゑひもせず
  "/utf8>>},
        {<<"langs.japanese_kanji">>, <<"
    色は匂へど 散りぬるを
    我が世誰ぞ 常ならむ
    有為の奥山 今日越えて
    浅き夢見じ 酔ひもせず
  "/utf8>>},
         {<<"langs.английский"/utf8>>, <<"The quick brown fox jumps over the lazy dog.">>},
         {<<"langs.chinese">>, <<"
    花非花
    雾非雾
    夜半来
    天明去
    来如春梦几多时
    去似朝云无觅处
  "/utf8>>},
        {<<"langs.chinese@作者"/utf8>>, <<"Bai Juyi">>},
        {<<"langs.chinese@title"/utf8>>, <<"The Bloom is not a Bloom">>}],
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:keysort(1, Expect), lists:keysort(1, Result)),
    [?assertPairsEq(E,R) || {E,R} <- Pairs],
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
