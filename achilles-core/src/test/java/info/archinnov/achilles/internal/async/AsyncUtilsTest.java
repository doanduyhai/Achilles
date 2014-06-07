package info.archinnov.achilles.internal.async;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.ExecutorService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class AsyncUtilsTest {

    @InjectMocks
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private FutureCallback<Object> callBack;

    @Mock
    private ListenableFuture<Object> listenableFuture;

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    @Mock
    private Function<Object, Object> function;

    @Test
    public void should_add_async_listener_from_options() throws Exception {
        //Given
        final Options options = OptionsBuilder.withAsyncListeners(callBack);
        when(listenableFuture.get()).thenReturn("test");
        //When
        asyncUtils.maybeAddAsyncListeners(listenableFuture, options, executorService);

        //Then
        verify(listenableFuture).addListener(runnableCaptor.capture(), eq(executorService));

        final Runnable runnable = runnableCaptor.getValue();
        runnable.run();

        verify(callBack).onSuccess("test");
    }

    @Test
    public void should_add_async_listeners() throws Exception {
        //Given
        when(listenableFuture.get()).thenReturn("test");

        //When
        asyncUtils.maybeAddAsyncListeners(listenableFuture, new FutureCallback[] { callBack }, executorService);

        //Then
        verify(listenableFuture).addListener(runnableCaptor.capture(), eq(executorService));

        final Runnable runnable = runnableCaptor.getValue();
        runnable.run();

        verify(callBack).onSuccess("test");
    }

}
