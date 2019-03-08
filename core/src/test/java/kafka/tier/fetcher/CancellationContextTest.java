package kafka.tier.fetcher;

import org.junit.Assert;
import org.junit.Test;

public class CancellationContextTest {

    @Test
    public void cancellationChain() {
        CancellationContext rootContext = CancellationContext.newContext();
        CancellationContext l1 = rootContext.subContext();
        CancellationContext l2 = l1.subContext();
        CancellationContext l3 = l2.subContext();

        l3.cancel();
        Assert.assertTrue("Canceling a context works", l3.isCancelled());
        Assert.assertFalse("Canceling the lowest context does not cancel the higher contexts", l2.isCancelled());
    }
}
