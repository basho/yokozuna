%% -*- coding: utf-8 -*-
-module(yz_xml_extractor_tests).
-compile(export_all).
-include_lib("yz_test.hrl").

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
         {<<"langs.irish">>, <<"\"An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí do leasa ṫú?\" \"D'ḟuascail Íosa Úrṁac na hÓiġe Beannaiṫe pór Éava agus Áḋaiṁ.\"">>},
         {<<"langs.dutch">>, <<"Pa's wĳze lynx bezag vroom het fikse aquaduct.">>},
         {<<"langs.german_1">>, <<"Falsches Üben von Xylophonmusik quält jeden größeren Zwerg.">>},
         {<<"langs.german_1@attr">>, <<"Falsches Üben">>},
         {<<"langs.german_2">>, <<"Im finſteren Jagdſchloß am offenen Felsquellwaſſer patzte der affig-flatterhafte kauzig-höf‌liche Bäcker über ſeinem verſifften kniffligen C-Xylophon.">>},
         {<<"langs.norwegian">>, <<"Blåbærsyltetøy.">>},
         {<<"langs.danish">>, <<"Høj bly gom vandt fræk sexquiz på wc.">>},
         {<<"langs.swedish">>, <<"Flygande bäckasiner söka strax hwila på mjuka tuvor.">>},
         {<<"langs.icelandic">>, <<"Sævör grét áðan því úlpan var ónýt.">>},
         {<<"langs.finnish">>, <<"Törkylempijävongahdus.">>},
         {<<"langs.polish">>, <<"Pchnąć w tę łódź jeża lub osiem skrzyń fig.">>},
         {<<"langs.czech">>, <<"Příliš žluťoučký kůň úpěl ďábelské kódy.">>},
         {<<"langs.slovak">>, <<"Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote.">>},
         {<<"langs.greek_monotonic">>, <<"ξεσκεπάζω την ψυχοφθόρα βδελυγμία">>},
         {<<"langs.greek_polytonic">>, <<"ξεσκεπάζω τὴν ψυχοφθόρα βδελυγμία">>},
         {<<"langs.russian">>, <<"Съешь же ещё этих мягких французских булок да выпей чаю.">>},
         {<<"langs.bulgarian">>, <<"Жълтата дюля беше щастлива, че пухът, който цъфна, замръзна като гьон.">>},
         {<<"langs.sami">>, <<"Vuol Ruoŧa geđggiid leat máŋga luosa ja čuovžža.">>},
         {<<"langs.hungarian">>, <<"Árvíztűrő tükörfúrógép.">>},
         {<<"langs.spanish">>, <<"El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro.">>},
         {<<"langs.portuguese">>, <<"O próximo vôo à noite sobre o Atlântico, põe freqüentemente o único médico.">>},
         {<<"langs.french">>, <<"Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être déçus en voyant leurs drôles d'œufs abîmés.">>},
         {<<"langs.esperanto">>, <<"Eĥoŝanĝo ĉiuĵaŭde.">>},
         {<<"langs.hebrew">>, <<"זה כיף סתם לשמוע איך תנצח קרפד עץ טוב בגן.">>},
         {<<"langs.japanese_hiragana">>, <<"
    いろはにほへど　ちりぬるを
    わがよたれぞ　つねならむ
    うゐのおくやま　けふこえて
    あさきゆめみじ　ゑひもせず
  ">>},
        {<<"langs.japanese_kanji">>, <<"
    色は匂へど 散りぬるを
    我が世誰ぞ 常ならむ
    有為の奥山 今日越えて
    浅き夢見じ 酔ひもせず
  ">>},
         {<<"langs.английский">>, <<"The quick brown fox jumps over the lazy dog.">>},
         {<<"langs.chinese">>, <<"
    花非花
    雾非雾
    夜半来
    天明去
    来如春梦几多时
    去似朝云无觅处
  ">>},
        {<<"langs.chinese@作者">>, <<"Bai Juyi">>},
        {<<"langs.chinese@title">>, <<"The Bloom is not a Bloom">>}],
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:keysort(1, Expect), lists:keysort(1, Result)),
    [?assertPairsEq(E,R) || {E,R} <- Pairs],
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
