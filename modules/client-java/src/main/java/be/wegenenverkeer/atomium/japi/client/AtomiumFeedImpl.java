package be.wegenenverkeer.atomium.japi.client;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.reactivex.rxjava3.core.Flowable.fromIterable;

class AtomiumFeedImpl<E> implements AtomiumFeed<E> {
    private final static Logger logger = LoggerFactory.getLogger(AtomiumFeedImpl.class);
    private final PageFetcher<E> pageFetcher;

    AtomiumFeedImpl(PageFetcher<E> pageFetcher) {
        this.pageFetcher = pageFetcher;
    }

    @Override
    public Flowable<FeedEntry<E>> from(final String entryId, final String pageUrl) {
        return fetchEntries(pageUrl, Optional.empty());
    }

    @Override
    public Flowable<FeedEntry<E>> fromNowOn() {
        return fetchEntries(AtomiumFeed.FIRST_PAGE, Optional.empty());
    }

    @Override
    public Flowable<FeedEntry<E>> fromBeginning() {
        return fetchFirstPage()
                .toFlowable()
                .flatMap(lastPage -> fetchEntries(lastPage.getLastHref(), Optional.empty()));
    }

    private Flowable<FeedEntry<E>> fetchEntries(String pageUrl, Optional<String> eTag) {
        return fetchPage(pageUrl, eTag)
                .toFlowable()
                .flatMap(page ->
                        this.parseEntries(page)
                                .concatWith(Flowable.just("")
                                        .delay(pageFetcher.getPollingInterval().toMillis(), TimeUnit.MILLISECONDS)
                                        .doOnNext(delay -> logger.debug("Waited {}ms to fetch more entries.", pageFetcher.getPollingInterval().toMillis()))
                                        .flatMap(delay -> fetchEntries(previousOrSelfHref(page), page.getEtag()))
                                )
                );
    }

    private Single<CachedFeedPage<E>> fetchFirstPage() {
        return fetchPage(AtomiumFeed.FIRST_PAGE, Optional.empty());
    }

    private Single<CachedFeedPage<E>> fetchPage(String pageUrl, Optional<String> eTag) {
        return pageFetcher.fetch(pageUrl, eTag);
    }

    private Flowable<FeedEntry<E>> parseEntries(CachedFeedPage<E> page) {
        return fromIterable(page.getEntries())
                .map(entry -> new FeedEntry<>(entry, page));
    }

    private String previousOrSelfHref(CachedFeedPage<E> page) {
        return page.getPreviousHref().orElseGet(page::getSelfHref);
    }
}