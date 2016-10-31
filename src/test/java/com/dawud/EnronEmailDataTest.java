package com.dawud;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit test for Action and Transformations.
 */
public class EnronEmailDataTest extends SharedJavaSparkContext implements Serializable {


    @Test
    public void testAction_getWordCount() {
        final String in =
                "Please let me know if you still need Curve Shift.\n" +
                        "\n" +
                        "Thanks,\n" +
                        "Heather";
        final long expected = 12L;

        Assert.assertEquals("Should have word count:",
                expected,
                EnronEmailData.action_getWordCount(in)
        );
    }

    @Test
    public void testAction_getWordCountJunkTextExclusion() {
        final String in = "Please let me know if you still need Curve Shift.\n" +
                "\n" +
                "Thanks,\n" +
                "Heather\n" +
                " -----Original Message-----";
        final long expected = 12L;

        Assert.assertEquals("Should have word count:",
                expected,
                EnronEmailData.action_getWordCount(in)
        );
    }

    @Test
    public void testAction_getWordCountAttachmentsExclusion() {
        final String in =
                " -----Original Message-----\n" +
                        "From: \tDunton, Heather  \n" +
                        "Sent:\tTuesday, December 04, 2001 3:12 PM\n" +
                        "To:\tBelden, Tim; Allen, Phillip K.\n" +
                        "Cc:\tDriscoll, Michael M.\n" +
                        "Subject:\tWest Position\n" +
                        "\n" +
                        " << File: west_delta_pos.xls >> \n" +
                        "\n" +
                        "Let me know if you have any questions.\n" +
                        "\n" +
                        "Heather";
        final long expected = 30L;

        Assert.assertEquals("Should have word count:",
                expected,
                EnronEmailData.action_getWordCount(in)
        );
    }

    @Test
    public void testActiong_getWordCountNumbersExclusion() {
        final String in =
                " -----Original Message-----\n" +
                "From: \tDunton, Heather  \n" +
                "Sent:\tTuesday, December 04, 2001 3:12 PM\n" +
                "To:\tBelden, Tim; Allen, Phillip K.\n" +
                "Cc:\tDriscoll, Michael M.\n" +
                "Subject:\tWest Position\n" +
                "\n" +
                "\n" +
                "Attached is the Delta position for 1/18, 1/31, 6/20, 7/16, 9/24\n" +
                "\n" +
                "Let me know if you have any questions.\n" +
                "\n" +
                "Heather";

        final long expected = 35L;

        Assert.assertEquals("Should have word count:",
                expected,
                EnronEmailData.action_getWordCount(in)
        );
    }


    @Test
    public void testTransform_stripHeaders() {
        final String in = "Date: Thu, 24 Jan 2002 04:56:46 -0800 (PST)\n" +
                "From: Schedule Crawler <pete.davis@enron.com>\n" +
                "To: pete.davis@enron.com\n" +
                "CC: bert.meyers@enron.com, bill.williams.III@enron.com, Craig.Dean@enron.com, Geir.Solberg@enron.com, john.anderson@enron.com, mark.guzman@enron.com, michael.mier@enron.com, pete.davis@enron.com, ryan.slinger@enron.com\n" +
                "Subject: Schedule Crawler: HourAhead Failure\n" +
                "X-SDOC: 76339\n" +
                "X-ZLID: zl-edrm-enron-v2-meyers-a-358.eml" +
                "\n" +
                " \n" +
                "Please let me know if you still need Curve Shift.\n" +
                "\n" +
                "Thanks,\n" +
                "Heather";

        final List<String> expected =
                Arrays.asList("Please", "let", "me", "know", "if", "you", "still", "need", "Curve", "Shift", "Thanks", "Heather");

        Assert.assertEquals(
                "Header should be stripped",
                expected,
                EnronEmailData.transform_stripHeaders(in)
        );
    }

    @Test
    public void testTransform_stripOriginalMessageHead() {
        final String in =
                "----- Original Message -----  \n" +
                "From: Stephen  Bilby  \n" +
                "To: wes.colwell@enron.com, georgeanne.hodges@enron.com, rob.milnthorp@enron.com, john.zufferli@enron.com, peggy.hedstrom@enron.com, thomas.myers@enron.com, s..bradford@enron.com, lloyd.will@enron.com, sally.beck@enron.com, m.hall@enron.com, m..presto@enron.com, david.forster@enron.com, leslie.reeves@enron.com, chris.gaskill@enron.com, robert.superty@enron.com,fred.lagrasta@enron.com, laura.luce@enron.com, barry.tycholiz@enron.com, brian.redmond@enron.com, frank.vickers@enron.com, c..gossett@enron.com, john.arnold@enron.com, mike.grigsby@enron.com, k..allen@enron.com, scott.neal@enron.com, a..martin@enron.com, s..shively@enron.com, rita.wynne@enron.com, jenny.rub@enron.com, jay.webb@enron.com, e..haedicke@enron.com, rick.buy@enron.com, f..calger@enron.com, david.duran@enron.com, mitch.robinson@enron.com, mike.curry@enron.com, tim.heizenrader@enron.com, tim.belden@enron.com, w..white@enron.com, d..steffes@enron.com, c..aucoin@enron.com, a..roberts@enron.com, david.oxley@enron.com, john.lavorato@enron.com\n" +
                "Cc:\tDriscoll, Michael M.\n" +
                "Bcc:\tDriscoll, Michael M.\n" +
                "Sent: Monday, October 29, 2001 9:03 AM\n" +
                "Subject: Golfin' and Wedding!!!\n" +
                "Hello  All,\n" +
                "I want to see who  will be able to golf on Friday and Saturday.\n" +
                "Friday we will golf  here in San Antonio and everything is taken care of already.  It will be at  the Quarry and I think the times are between 8 adn 8:30 am. \n" +
                "Saturday we will  golf at the Shreiner Public Golf Course and we have a tee time at 8am and  8:07am.  Fee for this this one I believe is  $33.00/person.\n";

        final String expected =
                        "\n" +
                        "Hello  All,\n" +
                        "I want to see who  will be able to golf on Friday and Saturday.\n" +
                        "Friday we will golf  here in San Antonio and everything is taken care of already.  It will be at  the Quarry and I think the times are between 8 adn 8:30 am. \n" +
                        "Saturday we will  golf at the Shreiner Public Golf Course and we have a tee time at 8am and  8:07am.  Fee for this this one I believe is  $33.00/person.\n";

        Assert.assertEquals(
                "Original Message Head should be stripped",
                expected,
                EnronEmailData.transform_stripOriginalMessageHead(in)
        );

    }



    @Test
    public void testAction_wordLengthAverage() {
        final String in =
                "Date: Mon, 21 Jan 2002 01:36:41 -0800 (PST)\n" +
                        "From: Schedule Crawler <pete.davis@enron.com>\n" +
                        "To: wes.colwell@enron.com, georgeanne.hodges@enron.com, rob.milnthorp@enron.com, john.zufferli@enron.com, peggy.hedstrom@enron.com, thomas.myers@enron.com, s..bradford@enron.com, lloyd.will@enron.com, sally.beck@enron.com, m.hall@enron.com, m..presto@enron.com, david.forster@enron.com, leslie.reeves@enron.com, chris.gaskill@enron.com, robert.superty@enron.com,fred.lagrasta@enron.com, laura.luce@enron.com, barry.tycholiz@enron.com, brian.redmond@enron.com, frank.vickers@enron.com, c..gossett@enron.com, john.arnold@enron.com, mike.grigsby@enron.com, k..allen@enron.com, scott.neal@enron.com, a..martin@enron.com, s..shively@enron.com, rita.wynne@enron.com, jenny.rub@enron.com, jay.webb@enron.com, e..haedicke@enron.com, rick.buy@enron.com, f..calger@enron.com, david.duran@enron.com, mitch.robinson@enron.com, mike.curry@enron.com, tim.heizenrader@enron.com, tim.belden@enron.com, w..white@enron.com, d..steffes@enron.com, c..aucoin@enron.com, a..roberts@enron.com, david.oxley@enron.com, john.lavorato@enron.com\n" +
                        "CC: bert.meyers@enron.com, bill.williams.III@enron.com, Craig.Dean@enron.com, Geir.Solberg@enron.com, john.anderson@enron.com, mark.guzman@enron.com, michael.mier@enron.com, pete.davis@enron.com, ryan.slinger@enron.com\n" +
                        "Subject: Schedule Crawler: HourAhead Failure\n" +
                        "X-SDOC: 76425\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-444.eml\n" +
                        "Hello  All,\n" +
                        "I want to see who  will be able to golf on Friday and Saturday.\n" +
                        "Friday we will golf  here in San Antonio and everything is taken care of already.  It will be at  the Quarry and I think the times are between 8 adn 8:30 am. \n" +
                        "Saturday we will  golf at the Shreiner Public Golf Course and we have a tee time at 8am and  8:07am.  Fee for this this one I believe is  $33.00/person.\n";

        final String in2 =
                "Date: Wed, 2 Jan 2002 14:57:08 -0800 (PST)\n" +
                        "Subject: HEALTH INSURANCE CARDS\n" +
                        "From: Rodriguez  Grace <Grace.Rodriguez@ENRON.com>\n" +
                        "To: DL-Portland World Trade Center <DL-PortlandWorldTradeCenter@ENRON.com>, bert.meyers@enron.com, bill.williams.III@enron.com\n" +
                        "X-SDOC: 76983\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-1001.eml\n" +
                        "\n" +
                        "Hello, and happy new year!";

        List<String> input = Arrays.asList(in, in2);
        JavaRDD<String> rddInput = jsc().parallelize(input);
        double actualAverage = EnronEmailData.action_wordLengthAverage(rddInput);
        double expected = 3.725;
        assertEquals("Should be equal to " + expected, expected, actualAverage, 0.1);

        // Create the expected output
//        List<Tuple2<Integer, Integer>> expectedInput = Arrays.asList(
//                new Tuple2<Integer, Integer>(1, 1),
//                new Tuple2<Integer, Integer>(1, 2));
//        JavaRDD<Tuple2<Integer, Integer>> expectedRDD = jsc().parallelize(expectedInput);

//        ClassTag<Tuple2<Integer, Integer>> tag =
//                scala.reflect.ClassTag$.MODULE$
//                        .apply(Tuple2.class);

        // Run the assertions on the result and expected
//        JavaRDDComparisons.assertRDDEquals(
//                JavaRDD.fromRDD(JavaRDD.toRDD(result), tag),
//                JavaRDD.fromRDD(JavaRDD.toRDD(expectedRDD), tag));
//        JavaRDDComparisons.assertRDDEquals(expectedRDD, actualRDD);



    }





    @Test
    public void testTransform_extractEmailToAddresses() {
        final String in =
                "Date: Mon, 21 Jan 2002 01:36:41 -0800 (PST)\n" +
                        "From: Schedule Crawler <pete.davis@enron.com>\n" +
                        "To: wes.colwell@enron.com, georgeanne.hodges@enron.com, rob.milnthorp@enron.com, john.zufferli@enron.com, david.oxley@enron.com, john.lavorato@enron.com\n" +
                        "CC: bert.meyers@enron.com, bill.williams.III@enron.com, Craig.Dean@enron.com, Geir.Solberg@enron.com, john.anderson@enron.com, mark.guzman@enron.com, michael.mier@enron.com, pete.davis@enron.com, ryan.slinger@enron.com\n" +
                        "Subject: Schedule Crawler: HourAhead Failure\n" +
                        "X-SDOC: 76425\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-444.eml";

        final String in2 =
                "Date: Wed, 2 Jan 2002 14:57:08 -0800 (PST)\n" +
                        "Subject: HEALTH INSURANCE CARDS\n" +
                        "From: Rodriguez  Grace <Grace.Rodriguez@ENRON.com>\n" +
                        "To: DL-Portland World Trade Center <DL-PortlandWorldTradeCenter@ENRON.com>, bert.meyers@enron.com, bill.williams.III@enron.com\n" +
                        "X-SDOC: 76983\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-1001.eml\n" +
                        "\n" +
                        "Hello, and happy new year!";

        final List<String> expected = new ArrayList<>();
        expected.add("wes.colwell@enron.com");
        expected.add("georgeanne.hodges@enron.com");
        expected.add("rob.milnthorp@enron.com");
        expected.add("john.zufferli@enron.com");
        expected.add("david.oxley@enron.com");
        expected.add("john.lavorato@enron.com");

        final List<String> expected2 = new ArrayList<>();
        expected2.add("DL-Portland World Trade Center <DL-PortlandWorldTradeCenter@ENRON.com>");
        expected2.add("bert.meyers@enron.com");
        expected2.add("bill.williams.III@enron.com");

        assertEquals("Should get extract of email addresses:",
                expected,
                EnronEmailData.transform_extractEmailAddresses(in, EnronEmailData.DELIMITER_TO_FIELD_NAME)
        );

        assertEquals("Should get extract one email addresses:",
                expected2,
                EnronEmailData.transform_extractEmailAddresses(in2, EnronEmailData.DELIMITER_TO_FIELD_NAME)
        );
    }


    @Test
    public void action_GetTop100Emails(){
        String in = "Date: Tue, 5 Feb 2002 07:56:56 -0800 (PST)\n" +
                "From: Schedule Crawler <pete.davis@enron.com>\n" +
                "To: one.email@enron, pete.davis@enron.com\n" +
                "CC: bert.meyers@enron.com, bill.williams.III@enron.com, Craig.Dean@enron.com, Geir.Solberg@enron.com, john.anderson@enron.com, mark.guzman@enron.com, michael.mier@enron.com, pete.davis@enron.com, ryan.slinger@enron.com\n" +
                "Subject: Schedule Crawler: HourAhead Failure\n" +
                "X-SDOC: 76014\n" +
                "X-ZLID: zl-edrm-enron-v2-meyers-a-33.eml\n" +
                "\n" +
                "\n" +
                "\n" +
                "Start Date: 2/5/02; HourAhead hour: 10;  HourAhead schedule download failed. Manual intervention required.\n" +
                "\n" +
                "***********\n" +
                "EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, please cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\n" +
                "***********\n";

        String expected = "(pete.davis@enron.com,1.5)\n" +
                "(john.anderson@enron.com,0.5)\n" +
                "(Geir.Solberg@enron.com,0.5)\n" +
                "(pete.davis@enron.com,0.5)\n" +
                "(mark.guzman@enron.com,0.5)\n" +
                "(michael.mier@enron.com,0.5)\n" +
                "(bill.williams.III@enron.com,0.5)\n" +
                "(bert.meyers@enron.com,0.5)\n" +
                "(Craig.Dean@enron.com,0.5)";

        final Map<String, Double> expectedList = new HashMap<String, Double>();
        expectedList.put("pete.davis@enron.com", 1.5);
        expectedList.put("bert.meyers@enron.com", 0.5);
        expectedList.put("mark.guzman@enron.com", 0.5);
        expectedList.put("michael.mier@enron.com", 0.5);
        expectedList.put("john.anderson@enron.com", 0.5);
        expectedList.put("Geir.Solberg@enron.com", 0.5);
        expectedList.put("bill.williams.III@enron.com", 0.5);
        expectedList.put("ryan.slinger@enron.com", 0.5);
        expectedList.put("Craig.Dean@enron.com", 0.5);


        List<String> input = Arrays.asList(in);
        JavaRDD<String> rddInput = jsc().parallelize(input);

        Map<String, Double> actual = EnronEmailData.action_GetTop100Emails(rddInput);
        assertTrue(expectedList.size() >0);

        /*        actual.foreach( s ->
                System.out.println(s)
        );*/


//        rddInput.flatMap(flatMapFunctionTOEmail)
//                .filter(filterPredicateEmail)
//                .mapToPair(pairFunctionFullScore)
//                .foreach(s -> System.out.println("=======>"+ s +"<==="));
    }

    @Test
    public void testTransform_extractEmailCcAddresses() {
        final String in =
                "Date: Mon, 21 Jan 2002 01:36:41 -0800 (PST)\n" +
                        "From: Schedule Crawler <pete.davis@enron.com>\n" +
                        "To: pete.davis@enron.com\n" +
                        "CC: bert.meyers@enron.com, bill.williams.III@enron.com, Craig.Dean@enron.com, Geir.Solberg@enron.com, john.anderson@enron.com, mark.guzman@enron.com, michael.mier@enron.com, pete.davis@enron.com, ryan.slinger@enron.com\n" +
                        "Subject: Schedule Crawler: HourAhead Failure\n" +
                        "X-SDOC: 76425\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-444.eml";

        final String in2 =
                "Date: Wed, 2 Jan 2002 14:57:08 -0800 (PST)\n" +
                        "Subject: HEALTH INSURANCE CARDS\n" +
                        "From: Rodriguez  Grace <Grace.Rodriguez@ENRON.com>\n" +
                        "To: DL-Portland World Trade Center <DL-PortlandWorldTradeCenter@ENRON.com>, bert.meyers@enron.com, bill.williams.III@enron.com\n" +
                        "X-SDOC: 76983\n" +
                        "X-ZLID: zl-edrm-enron-v2-meyers-a-1001.eml\n" +
                        "\n" +
                        "Hello, and happy new year!";

        final List<String> expected = new ArrayList<>();
        expected.add("bert.meyers@enron.com");
        expected.add("bill.williams.III@enron.com");
        expected.add("Craig.Dean@enron.com");
        expected.add("Geir.Solberg@enron.com");
        expected.add("john.anderson@enron.com");
        expected.add("mark.guzman@enron.com");
        expected.add("michael.mier@enron.com");
        expected.add("pete.davis@enron.com");
        expected.add("ryan.slinger@enron.com");

        final List<String> expected2 = Arrays.asList();

        assertEquals("Should get extract of email addresses:",
                expected,
                EnronEmailData.transform_extractEmailAddresses(in, EnronEmailData.DELIMITER_CC_FIELD_NAME)
        );

        assertEquals("Should get extract none email addresses:",
                expected2,
                EnronEmailData.transform_extractEmailAddresses(in2, EnronEmailData.DELIMITER_CC_FIELD_NAME)
        );
    }


    @Test
    @Ignore
    public void testOpenReceivedMessageFolder() {
        String in = "C:\\Users\\dev\\Documents\\workspace\\data\\edrm-enron-v2\\pst_version";
        assertTrue("File list returned", EnronEmailData.openReceivedMessageFolder(in).size() >0);
    }

    @Test
    @Ignore
    public void testGetFilteredFileList() {
        String in = "C:\\Users\\dev\\Documents\\workspace\\data\\edrm-enron-v2\\pst_version";
        List<String> output = EnronEmailData.getFilteredFileList(in);
//        output.stream().filter(s -> (s.endsWith(".txt") || s.endsWith(".eml"))).forEach(s -> System.out.println(s));

//        System.out.println("\"" + String.join("\", \"", output) + "\"");
        assertNotNull("Filtered list returned", output);
        assertTrue("Filtered list returned", output.size() >0);
    }

}
