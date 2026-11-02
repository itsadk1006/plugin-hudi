package io.kestra.plugin.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Test only: serves the allowed remote scheme `s3a` from the local disk (registered in core-site.xml), so that
 * {@code Trigger.evaluate()} can run end to end without a remote file system. `s3a://bucket/a/b` maps to `/a/b`.
 */
public class FakeRemoteFileSystem extends RawLocalFileSystem {

    private URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    }

    @Override
    public URI getUri() {
        return uri != null ? uri : super.getUri();
    }
}
