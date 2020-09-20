package com.znwhahaha;


import java.io.IOException;

public class App
{
    public static void main( String[] args ) throws IOException {
        FromHbase fromHbase = new FromHbase();
        fromHbase.setup();
//        fromHbase.queryTableFromPersonProfile("PersonProfile");
//        fromHbase.queryTableFromInstrument("pf_instrument");
//        fromHbase.queryTbaleFromAchievements("pf_achievement_award");
        fromHbase.queryNewTableFromPersonProfile("PersonProfile");
        fromHbase.queryNewTableFromInstrument("pf_instrument");
        fromHbase.queryNewTbaleFromAchievements("pf_achievement_award");
        fromHbase.close();
    }
}
