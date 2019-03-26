// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client;

import io.confluent.security.auth.client.rest.UrlSelector;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class UrlSelectorTest {

    @Test
    public void verifyUrlRotation() {
        String url1 = "http://test1.com";
        String url2 = "http://test2.com";
        String url3 = "http://test3.com";
        List<String> urlList = Arrays.asList(url1, url2, url3);

        UrlSelector urlSelector = new UrlSelector(urlList);
        assertEquals(urlList.size(), urlSelector.size());

        int startIndex = urlSelector.index();
        int currentIndex = startIndex;
        assertEquals(urlList.get(currentIndex), urlSelector.current());

        for (int i = 0, n = urlSelector.size(); i < n; i++) {
            urlSelector.fail();
            currentIndex = urlSelector.index();
            assertEquals(urlList.get(currentIndex), urlSelector.current());
        }

        assertEquals(startIndex, urlSelector.index());
    }

}
