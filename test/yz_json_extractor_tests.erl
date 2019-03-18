-module(yz_json_extractor_tests).
-compile(export_all).
-include_lib("yz_test.hrl").

-spec b(string()) -> unicode:unicode_binary().
b(Str) -> unicode:characters_to_binary(Str).

json_extract_test() ->
    {ok, TestJSON} = file:read_file("test/test.json"),
    Result = yz_json_extractor:extract(TestJSON),
    Expect =
        [{<<"name">>,<<"ryan">>},
         {<<"age">>,<<"29">>},
         {<<"pets">>,<<"smokey">>},
         {<<"pets">>,<<"bandit">>},
         {<<"books.title">>,   b("Introduction to Information Retrieval")},
         {<<"books.title">>,   b("Principles of Distributed Database Systems")},
         {<<"books.authors">>, b("Christopher D. Manning")},
         {<<"books.authors">>, b("Prabhakar Raghavan")},
         {<<"books.authors">>, b("Hinrich Schütze")},
         {<<"books.authors">>, b("M. Tamer Özsu")},
         {<<"books.authors">>, b("Patrick Valduriez")},
         {<<"alive">>,true},
         {<<"married">>,false},
         {<<"a_number">>,<<"1100000.0">>},
         {<<"lucky_numbers">>,<<"13">>},
         {<<"lucky_numbers">>,<<"17">>},
         {<<"lucky_numbers">>,<<"21">>}],

    %% Do one at a time so failure is easier to understand
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:sort(Expect), lists:sort(Result)),
    [?assertEqual(E,R) || {E,R} <- Pairs],
    %% Verify conversion doesn't error
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).

utf8_test() ->
    {ok, JSON} = file:read_file("test/utf8.json"),
    Result = yz_json_extractor:extract(JSON),
    case Result of
        {error, Reason} ->
            ?debugFmt("~nextract/1 failed: ~s~n", [Reason]),
            throw(extract_failed);
        _ ->
            ok
    end,
    Strs =
        [{"langs.english",  "The quick brown fox jumps over the lazy dog."},
         {"langs.jamaican", "Chruu, a kwik di kwik brong fox a jomp huova di liezi daag de, yu no siit?"},
         {"langs.irish",    "\"An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí do leasa ṫú?\" \"D'ḟuascail Íosa Úrṁac na hÓiġe Beannaiṫe pór Éava agus Áḋaiṁ.\""},
         {"langs.dutch",    "Pa's wĳze lynx bezag vroom het fikse aquaduct."},
         {"langs.german_1", "Falsches Üben von Xylophonmusik quält jeden größeren Zwerg."},
         {"langs.german_2", "Im finſteren Jagdſchloß am offenen Felsquellwaſſer patzte der affig-flatterhafte kauzig-höf‌liche Bäcker über ſeinem verſifften kniffligen C-Xylophon."},
         {"langs.norwegian", "Blåbærsyltetøy."},
         {"langs.danish",    "Høj bly gom vandt fræk sexquiz på wc."},
         {"langs.swedish",   "Flygande bäckasiner söka strax hwila på mjuka tuvor."},
         {"langs.icelandic", "Sævör grét áðan því úlpan var ónýt."},
         {"langs.finnish",   "Törkylempijävongahdus."},
         {"langs.polish",    "Pchnąć w tę łódź jeża lub osiem skrzyń fig."},
         {"langs.czech",     "Příliš žluťoučký kůň úpěl ďábelské kódy."},
         {"langs.slovak",    "Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote."},
         {"langs.greek_monotonic", "ξεσκεπάζω την ψυχοφθόρα βδελυγμία"},
         {"langs.greek_polytonic", "ξεσκεπάζω τὴν ψυχοφθόρα βδελυγμία"},
         {"langs.russian",   "Съешь же ещё этих мягких французских булок да выпей чаю."},
         {"langs.bulgarian", "Жълтата дюля беше щастлива, че пухът, който цъфна, замръзна като гьон."},
         {"langs.sami",      "Vuol Ruoŧa geđggiid leat máŋga luosa ja čuovžža."},
         {"langs.hungarian", "Árvíztűrő tükörfúrógép."},
         {"langs.spanish",   "El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro."},
         {"langs.portuguese", "O próximo vôo à noite sobre o Atlântico, põe freqüentemente o único médico."},
         {"langs.french",    "Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être déçus en voyant leurs drôles d'œufs abîmés."},
         {"langs.esperanto", "Eĥoŝanĝo ĉiuĵaŭde."},
         {"langs.hebrew",    "זה כיף סתם לשמוע איך תנצח קרפד עץ טוב בגן."},
         {"langs.japanese_hiragana", "
    いろはにほへど　ちりぬるを
    わがよたれぞ　つねならむ
    うゐのおくやま　けふこえて
    あさきゆめみじ　ゑひもせず
  "},
        {"langs.japanese_kanji", "
    色は匂へど 散りぬるを
    我が世誰ぞ 常ならむ
    有為の奥山 今日越えて
    浅き夢見じ 酔ひもせず
  "},
         {"langs.английский", "The quick brown fox jumps over the lazy dog."},
         {"langs.chinese", "
    花非花
    雾非雾
    夜半来
    天明去
    来如春梦几多时
    去似朝云无觅处
  "}],
    Expect = [ {b(K), b(V)} || {K,V} <- Strs ],
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:keysort(1, Expect), lists:keysort(1, Result)),
    [?assertPairsEq(E,R) || {E,R} <- Pairs],
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
