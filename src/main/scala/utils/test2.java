package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;

/**
 * Created by Administrator on 2018/2/27.
 */
public class test2 {
    public static void main(String[] args) {

        HashSet <String> set = new HashSet<String>();
        set.add("b");
        set.add("a");
        set.add("c");
        set.add("d");
        set.add("g");
        set.add("f");
        set.add("e");
        set.add("e");
        for (String a :
                set) {
            System.out.println(a);
        }




    }
}
