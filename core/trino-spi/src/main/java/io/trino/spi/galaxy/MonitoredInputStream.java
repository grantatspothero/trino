/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.galaxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

class MonitoredInputStream
        extends InputStream
{
    private final NetworkMonitor networkMonitor;
    private final InputStream delegate;

    MonitoredInputStream(NetworkMonitor networkMonitor, InputStream delegate)
    {
        this.networkMonitor = requireNonNull(networkMonitor, "networkMonitor is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public int read()
            throws IOException
    {
        int readSize = delegate.read();
        networkMonitor.recordReadBytes(readSize);
        return readSize;
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        int readSize = delegate.read(b);
        networkMonitor.recordReadBytes(readSize);
        return readSize;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        int readSize = delegate.read(b, off, len);
        networkMonitor.recordReadBytes(readSize);
        return readSize;
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        byte[] bytes = delegate.readAllBytes();
        networkMonitor.recordReadBytes(bytes.length);
        return bytes;
    }

    @Override
    public byte[] readNBytes(int len)
            throws IOException
    {
        byte[] bytes = delegate.readNBytes(len);
        networkMonitor.recordReadBytes(bytes.length);
        return bytes;
    }

    @Override
    public int readNBytes(byte[] b, int off, int len)
            throws IOException
    {
        int readSize = delegate.readNBytes(b, off, len);
        networkMonitor.recordReadBytes(readSize);
        return readSize;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        long skipSize = delegate.skip(n);
        networkMonitor.recordReadBytes(skipSize);
        return skipSize;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        delegate.skipNBytes(n);
        networkMonitor.recordReadBytes(n);
    }

    @Override
    public int available()
            throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public void mark(int readLimit)
    {
        delegate.mark(readLimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        delegate.reset();
    }

    @Override
    public boolean markSupported()
    {
        return delegate.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        long transferSize = delegate.transferTo(out);
        networkMonitor.recordReadBytes(transferSize);
        return transferSize;
    }
}
