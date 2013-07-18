-module(yz_text_extractor_tests).
-compile(export_all).
-include_lib("yz_test.hrl").

-define(UTF8_EXPECT,
    <<"english: The quick brown fox jumps over the lazy dog.
jamaican: Chruu, a kwik di kwik brong fox a jomp huova di liezi daag de, yu no siit?
irish: \"An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí do leasa ṫú?\" \"D'ḟuascail Íosa Úrṁac na hÓiġe Beannaiṫe pór Éava agus Áḋaiṁ.\"
dutch: Pa's wĳze lynx bezag vroom het fikse aquaduct.
german_1: Falsches Üben von Xylophonmusik quält jeden größeren Zwerg.
german_2: Im finſteren Jagdſchloß am offenen Felsquellwaſſer patzte der affig-flatterhafte kauzig-höf‌liche Bäcker über ſeinem verſifften kniffligen C-Xylophon.
norwegian: Blåbærsyltetøy.
danish: Høj bly gom vandt fræk sexquiz på wc.
swedish: Flygande bäckasiner söka strax hwila på mjuka tuvor.
icelandic: Sævör grét áðan því úlpan var ónýt.
finnish: Törkylempijävongahdus.
polish: Pchnąć w tę łódź jeża lub osiem skrzyń fig.
czech: Příliš žluťoučký kůň úpěl ďábelské kódy.
slovak: Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote.
greek_monotonic: ξεσκεπάζω την ψυχοφθόρα βδελυγμία
greek_polytonic: ξεσκεπάζω τὴν ψυχοφθόρα βδελυγμία
russian: Съешь же ещё этих мягких французских булок да выпей чаю.
bulgarian: Жълтата дюля беше щастлива, че пухът, който цъфна, замръзна като гьон.
sami: Vuol Ruoŧa geđggiid leat máŋga luosa ja čuovžža.
hungarian: Árvíztűrő tükörfúrógép.
spanish: El pingüino Wenceslao hizo kilómetros bajo exhaustiva lluvia y frío, añoraba a su querido cachorro.
portuguese: O próximo vôo à noite sobre o Atlântico, põe freqüentemente o único médico.
french: Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être déçus en voyant leurs drôles d'œufs abîmés.
esperanto: Eĥoŝanĝo ĉiuĵaŭde.
hebrew: זה כיף סתם לשמוע איך תנצח קרפד עץ טוב בגן.
japanese_hiragana:
    いろはにほへど　ちりぬるを
    わがよたれぞ　つねならむ
    うゐのおくやま　けふこえて
    あさきゆめみじ　ゑひもせず
japanese_kanji:
    色は匂へど 散りぬるを
    我が世誰ぞ 常ならむ
    有為の奥山 今日越えて
    浅き夢見じ 酔ひもせず
английский: The quick brown fox jumps over the lazy dog.
chinese:
    花非花
    雾非雾
    夜半来
    天明去
    来如春梦几多时
    去似朝云无觅处
">>).


utf8_test() ->
    {ok, Txt} = file:read_file("../test/utf8.txt"),
    Result = yz_text_extractor:extract(Txt),
    case Result of
        {error, Reason} ->
            ?debugFmt("~nextract/1 failed: ~s~n", [Reason]),
            throw(extract_failed);
        _ ->
            ok
    end,
    ?assertEqual([{text, ?UTF8_EXPECT}], Result),
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
