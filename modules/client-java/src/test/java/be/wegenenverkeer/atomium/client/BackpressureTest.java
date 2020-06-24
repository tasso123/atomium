package be.wegenenverkeer.atomium.client;

import be.wegenenverkeer.atomium.client.rxhttpclient.RxHttpAtomiumClient;
import be.wegenenverkeer.rxhttpclient.rxjava.RxJavaHttpClient;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ClasspathFileSource;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static be.wegenenverkeer.atomium.client.FeedPositionStrategies.from;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.slf4j.LoggerFactory.getLogger;

public class BackpressureTest {
    private final static ClasspathFileSource WIREMOCK_MAPPINGS = new ClasspathFileSource("basis-scenario");
    private static final Logger LOG = getLogger(BackpressureTest.class);

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule(
            wireMockConfig()
                    .fileSource(WIREMOCK_MAPPINGS)
                    .notifier(new Slf4jNotifier(true))
    );

    @Rule
    public WireMockClassRule instanceRule = wireMockRule;

    private RxHttpAtomiumClient client;

    @Before
    public void before() {
        client = new RxHttpAtomiumClient(new RxJavaHttpClient.Builder()
                .setBaseUrl("http://localhost:8080/")
                .build());

        //reset WireMock so it will serve the events feed
        WireMock.resetToDefault();
    }

    @After
    public void after() {
        client.close();
    }

    @Test
    public void testProcessing_sameThread() {
        client.feed(client.getPageFetcherBuilder("/feeds/events", Event.class).setAcceptXml().build())
                .fetchEntries(from("20/forward/10", "urn:uuid:8641f2fd-e8dc-4756-acf2-3b708080ea3a"))
                .concatMap(event -> Flowable.just(event)
                        .doOnNext(myEvent -> Thread.sleep(Duration.ofSeconds(10).toMillis())))
                .test()
                .awaitCount(3)
                .assertNoErrors()
                .assertValueCount(3);

        // only 1 page is queried
        WireMock.verify(exactly(1), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/20/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/30/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/40/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/50/forward/10")).withHeader("Accept", equalTo("application/xml")));
    }

    @Test
    public void testProcessing_otherThread() {
        Publisher<FeedEntry<Event>> feedEntryPublisher = client.feed(client.getPageFetcherBuilder("/feeds/events", Event.class).setAcceptXml().build())
                .fetchEntries2(from("20/forward/10", "urn:uuid:8641f2fd-e8dc-4756-acf2-3b708080ea3a"))
                .doOnNext(eventFeedEntry -> {
                    LOG.info("FeedEntry0 : {}", eventFeedEntry);
                });

        Flux.from(feedEntryPublisher)
                .log()
                .doOnNext(eventFeedEntry -> {
                    LOG.info("FeedEntry1 : {}", eventFeedEntry);
                })
                .concatMap(event -> Flowable.just(event)
                        .doOnNext(myEvent -> Thread.sleep(Duration.ofSeconds(10).toMillis()))
                        .subscribeOn(Schedulers.newThread()), 1)
                .doOnNext(eventFeedEntry -> LOG.info("FeedEntry2 : {}", eventFeedEntry))
                .blockLast();

        // only 1 page is queried
        WireMock.verify(exactly(1), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/20/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/30/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/40/forward/10")).withHeader("Accept", equalTo("application/xml")));
        WireMock.verify(exactly(0), WireMock.getRequestedFor(WireMock.urlPathEqualTo("/feeds/events/50/forward/10")).withHeader("Accept", equalTo("application/xml")));
    }
}

