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
package io.trino.plugin.base.galaxy;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class RedirectingSSLSocket
        extends SSLSocket
{
    private final SSLSocket socket;
    private final GalaxySqlSocketFactory.SocketWrapperAndVerifier socketWrapper;

    public RedirectingSSLSocket(SSLSocket socket, GalaxySqlSocketFactory.SocketWrapperAndVerifier socketWrapper)
    {
        this.socket = requireNonNull(socket, "socket is null");
        this.socketWrapper = requireNonNull(socketWrapper, "socketWrapper is null");
    }

    @Override
    public void connect(SocketAddress endpoint)
            throws IOException
    {
        this.socket.connect(redirectSocketAddress(endpoint));
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout)
            throws IOException
    {
        this.socket.connect(redirectSocketAddress(endpoint), timeout);
    }

    @Override
    public void bind(SocketAddress bindpoint)
            throws IOException
    {
        this.socket.bind(redirectSocketAddress(bindpoint));
    }

    @Override
    public InetAddress getInetAddress()
    {
        return this.socket.getInetAddress();
    }

    @Override
    public InetAddress getLocalAddress()
    {
        return this.socket.getLocalAddress();
    }

    @Override
    public int getPort()
    {
        return this.socket.getPort();
    }

    @Override
    public int getLocalPort()
    {
        return this.socket.getLocalPort();
    }

    @Override
    public SocketAddress getRemoteSocketAddress()
    {
        return this.socket.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress()
    {
        return this.socket.getLocalSocketAddress();
    }

    @Override
    public SocketChannel getChannel()
    {
        return this.socket.getChannel();
    }

    @Override
    public InputStream getInputStream()
            throws IOException
    {
        return socketWrapper.getInputStream(this.socket.getInputStream());
    }

    @Override
    public OutputStream getOutputStream()
            throws IOException
    {
        return socketWrapper.getOutputStream(this.socket.getOutputStream());
    }

    @Override
    public void setTcpNoDelay(boolean on)
            throws SocketException
    {
        this.socket.setTcpNoDelay(on);
    }

    @Override
    public boolean getTcpNoDelay()
            throws SocketException
    {
        return this.socket.getTcpNoDelay();
    }

    @Override
    public void setSoLinger(boolean on, int linger)
            throws SocketException
    {
        this.socket.setSoLinger(on, linger);
    }

    @Override
    public int getSoLinger()
            throws SocketException
    {
        return this.socket.getSoLinger();
    }

    @Override
    public void sendUrgentData(int data)
            throws IOException
    {
        this.socket.sendUrgentData(data);
    }

    @Override
    public void setOOBInline(boolean on)
            throws SocketException
    {
        this.socket.setOOBInline(on);
    }

    @Override
    public boolean getOOBInline()
            throws SocketException
    {
        return this.socket.getOOBInline();
    }

    @Override
    public void setSoTimeout(int timeout)
            throws SocketException
    {
        this.socket.setSoTimeout(timeout);
    }

    @Override
    public int getSoTimeout()
            throws SocketException
    {
        return this.socket.getSoTimeout();
    }

    @Override
    public void setSendBufferSize(int size)
            throws SocketException
    {
        this.socket.setSendBufferSize(size);
    }

    @Override
    public int getSendBufferSize()
            throws SocketException
    {
        return this.socket.getSendBufferSize();
    }

    @Override
    public void setReceiveBufferSize(int size)
            throws SocketException
    {
        this.socket.setReceiveBufferSize(size);
    }

    @Override
    public int getReceiveBufferSize()
            throws SocketException
    {
        return this.socket.getReceiveBufferSize();
    }

    @Override
    public void setKeepAlive(boolean on)
            throws SocketException
    {
        this.socket.setKeepAlive(on);
    }

    @Override
    public boolean getKeepAlive()
            throws SocketException
    {
        return this.socket.getKeepAlive();
    }

    @Override
    public void setTrafficClass(int tc)
            throws SocketException
    {
        this.socket.setTrafficClass(tc);
    }

    @Override
    public int getTrafficClass()
            throws SocketException
    {
        return this.socket.getTrafficClass();
    }

    @Override
    public void setReuseAddress(boolean on)
            throws SocketException
    {
        this.socket.setReuseAddress(on);
    }

    @Override
    public boolean getReuseAddress()
            throws SocketException
    {
        return this.socket.getReuseAddress();
    }

    @Override
    public void close()
            throws IOException
    {
        this.socket.close();
    }

    @Override
    public void shutdownInput()
            throws IOException
    {
        this.socket.shutdownInput();
    }

    @Override
    public void shutdownOutput()
            throws IOException
    {
        this.socket.shutdownOutput();
    }

    @Override
    public String toString()
    {
        return this.socket.toString();
    }

    @Override
    public boolean isConnected()
    {
        return this.socket.isConnected();
    }

    @Override
    public boolean isBound()
    {
        return this.socket.isBound();
    }

    @Override
    public boolean isClosed()
    {
        return this.socket.isClosed();
    }

    @Override
    public boolean isInputShutdown()
    {
        return this.socket.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown()
    {
        return this.socket.isOutputShutdown();
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth)
    {
        this.socket.setPerformancePreferences(connectionTime, latency, bandwidth);
    }

    @Override
    public <T> T getOption(SocketOption<T> name)
            throws IOException
    {
        return this.socket.getOption(name);
    }

    @Override
    public <T> Socket setOption(SocketOption<T> name, T value)
            throws IOException
    {
        return this.socket.setOption(name, value);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions()
    {
        return this.socket.supportedOptions();
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
        return this.socket.getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites()
    {
        return this.socket.getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] suites)
    {
        this.socket.setEnabledCipherSuites(suites);
    }

    @Override
    public String[] getSupportedProtocols()
    {
        return this.socket.getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols()
    {
        return this.socket.getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] protocols)
    {
        this.socket.setEnabledProtocols(protocols);
    }

    @Override
    public SSLSession getSession()
    {
        return this.socket.getSession();
    }

    @Override
    public SSLSession getHandshakeSession()
    {
        return this.socket.getHandshakeSession();
    }

    @Override
    public void addHandshakeCompletedListener(HandshakeCompletedListener listener)
    {
        this.socket.addHandshakeCompletedListener(listener);
    }

    @Override
    public void removeHandshakeCompletedListener(HandshakeCompletedListener listener)
    {
        this.socket.removeHandshakeCompletedListener(listener);
    }

    @Override
    public void startHandshake()
            throws IOException
    {
        this.socket.startHandshake();
    }

    @Override
    public void setUseClientMode(boolean mode)
    {
        this.socket.setUseClientMode(mode);
    }

    @Override
    public boolean getUseClientMode()
    {
        return this.socket.getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean need)
    {
        this.socket.setNeedClientAuth(need);
    }

    @Override
    public boolean getNeedClientAuth()
    {
        return this.socket.getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean want)
    {
        this.socket.setWantClientAuth(want);
    }

    @Override
    public boolean getWantClientAuth()
    {
        return this.socket.getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean flag)
    {
        this.socket.setEnableSessionCreation(flag);
    }

    @Override
    public boolean getEnableSessionCreation()
    {
        return this.socket.getEnableSessionCreation();
    }

    /**
     * Overriding getSSLParameters() and setSSLParameters() explicitly
     * The default impl on SSLSocket ignores some SSLParameters properties entirely
     * Specifically sniNames is ignored and this is needed for SSH tunnels with TLS
     */
    @Override
    public SSLParameters getSSLParameters()
    {
        return this.socket.getSSLParameters();
    }

    /**
     * Overriding getSSLParameters() and setSSLParameters() explicitly
     * The default impl on SSLSocket ignores some SSLParameters properties entirely
     * Specifically sniNames is ignored and this is needed for SSH tunnels with TLS
     */
    @Override
    public void setSSLParameters(SSLParameters sslParameters)
    {
        this.socket.setSSLParameters(sslParameters);
    }

    @Override
    public String getApplicationProtocol()
    {
        return this.socket.getApplicationProtocol();
    }

    @Override
    public String getHandshakeApplicationProtocol()
    {
        return this.socket.getHandshakeApplicationProtocol();
    }

    @Override
    public void setHandshakeApplicationProtocolSelector(BiFunction<SSLSocket, List<String>, String> selector)
    {
        this.socket.setHandshakeApplicationProtocolSelector(selector);
    }

    @Override
    public BiFunction<SSLSocket, List<String>, String> getHandshakeApplicationProtocolSelector()
    {
        return this.socket.getHandshakeApplicationProtocolSelector();
    }

    private SocketAddress redirectSocketAddress(SocketAddress socketAddress)
            throws IOException
    {
        return socketWrapper.redirectAddress(socketAddress);
    }
}
