package org.elasticsearch.plugin.nlpcn;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Objects;

/**
 * NamedXContentRegistry Holder
 *
 * @author shiyuan
 * @version V1.0
 * @since 2021-08-12 14:39
 */
public class NamedXContentRegistryHolder {

    private static final SetOnce<NamedXContentRegistry> xContentRegistry = new SetOnce<>();

    @Inject
    public NamedXContentRegistryHolder(NamedXContentRegistry xContentRegistry) {
        Objects.requireNonNull(xContentRegistry);
        NamedXContentRegistryHolder.xContentRegistry.set(xContentRegistry);
    }

    public static NamedXContentRegistry get() {
        return xContentRegistry.get();
    }
}
