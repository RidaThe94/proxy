package me.internalizable.numdrassl.server.transfer;

import com.hypixel.hytale.protocol.HostAddress;
import com.hypixel.hytale.protocol.packets.auth.ClientReferral;
import me.internalizable.numdrassl.api.chat.ChatMessageBuilder;
import me.internalizable.numdrassl.api.player.TransferResult;
import me.internalizable.numdrassl.config.BackendServer;
import me.internalizable.numdrassl.plugin.bridge.PlayerTransferBridgeResult;
import me.internalizable.numdrassl.server.ProxyCore;
import me.internalizable.numdrassl.server.health.BackendHealth;
import me.internalizable.numdrassl.server.health.BackendHealthManager;
import me.internalizable.numdrassl.session.ProxySession;
import me.internalizable.numdrassl.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Handles player transfers between backend servers.
 *
 * <p>Uses {@link ClientReferral} packets to instruct the client to disconnect
 * and reconnect to the proxy. When they reconnect with referral data,
 * the ReferralManager routes them to the target backend.</p>
 */
public final class PlayerTransfer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlayerTransfer.class);
    private static final int MAX_PORT = 32767;
    private final ProxyCore proxyCore;

    public PlayerTransfer(@Nonnull ProxyCore proxyCore) {
        this.proxyCore = Objects.requireNonNull(proxyCore, "proxyCore");
    }

    // ==================== Transfer Operations ====================

    /**
     * Transfers a player to a different backend server.
     */
    @Nonnull
    public CompletableFuture<TransferResult> transfer(
            @Nonnull ProxySession session,
            @Nonnull BackendServer targetBackend) {

        // 1. Safety Check
        if (session.isTransferring()) {
            return CompletableFuture.completedFuture(TransferResult.failed(TransferResult.Status.ALREADY_TRANSFERRING));
        }

        // 2. Set State
        session.setTransferring(true);

        // 3. Connect to backend
        return targetBackend.connect(session)
                .thenCompose(connectionResult -> {
                    if (!connectionResult.isSuccess()) {
                        session.setTransferring(false);
                        return CompletableFuture.completedFuture(
                                TransferResult.failed(TransferResult.Status.CONNECTION_FAILED)
                        );
                    }

                    // 4. Complete Transfer
                    return session.completeTransfer(targetBackend)
                            .thenApply(v -> {
                                session.setTransferring(false);
                                return TransferResult.success();
                            });
                })
                .exceptionally(throwable -> {
                    session.setTransferring(false);
                    return TransferResult.failed(TransferResult.Status.INTERNAL_ERROR);
                });
    }

    /**
     * Transfers a player to a backend server by name.
     */
    @Nonnull
    public CompletableFuture<TransferResult> transfer(@Nonnull ProxySession session, @Nonnull String backendName) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(backendName, "backendName");

        BackendServer backend = proxyCore.getConfig().getBackendByName(backendName);
        if (backend == null) {
            LOGGER.warn("Session {}: Backend {} not found in configuration", session.getSessionId(), backendName);
            return CompletableFuture.completedFuture(TransferResult.failure("Unknown backend server: " + backendName));
        }

        return transfer(session, backend);
    }

    private PlayerTransferBridgeResult firePlayerTransferEvent(ProxySession session, BackendServer targetBackend) {
        var apiProxy = proxyCore.getApiProxy();
        if (apiProxy == null) {
            return PlayerTransferBridgeResult.allow(targetBackend);
        }
        return apiProxy.getEventBridge().firePlayerTransferEvent(session, targetBackend);
    }

    // ==================== Transfer Execution (Client Referral Logic) ====================

    /**
     * NOTE: This method is currently unused by the main transfer() method above.
     * If you intend to use ClientReferral (disconnect/reconnect) instead of Proxy-Bridge,
     * you should call this method inside transfer() instead of targetBackend.connect().
     */
    private CompletableFuture<TransferResult> checkBackendAndTransfer(
            ProxySession session,
            BackendServer targetBackend) {

        BackendHealthManager cache = proxyCore.getBackendHealthManager();
        if (cache == null) {
            return CompletableFuture.completedFuture(
                    TransferResult.failure("Internal error: backend health cache not initialized")
            );
        }

        // 1. Get the health object (Synchronous)
        BackendHealth health = cache.get(targetBackend);

        // 2. Check the status
        boolean isServerAlive = (health != null && health.isAlive());

        // 3. Chain the logic
        return CompletableFuture.completedFuture(isServerAlive)
                .thenApply(alive -> {
                    if (!alive) {
                        LOGGER.warn("Session {}: Backend {} is offline", session.getSessionId(), targetBackend.getName());
                        return TransferResult.failure("Server is offline");
                    }
                    return executeTransfer(session, targetBackend);
                })
                .exceptionally(ex -> {
                    LOGGER.warn("Session {}: Backend {} is not reachable: {}",
                            session.getSessionId(), targetBackend.getName(), ex.getMessage());
                    return TransferResult.failure("Server is offline");
                });
    }

    private TransferResult executeTransfer(ProxySession session, BackendServer targetBackend) {
        // Validate session state
        Optional<TransferResult> validationError = validateTransfer(session, targetBackend);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        // Resolve proxy address
        HostAddress proxyAddress = resolveProxyAddress(session);
        if (proxyAddress == null) {
            return TransferResult.failure("Port exceeds maximum value for player transfers");
        }

        // Create and send referral
        logTransferStart(session, targetBackend);
        byte[] referralData = createReferralData(session, targetBackend);
        sendClientReferral(session, proxyAddress, referralData);

        return TransferResult.success("Transfer initiated");
    }

    // ==================== Validation ====================

    private Optional<TransferResult> validateTransfer(ProxySession session, BackendServer targetBackend) {
        if (session.getState() != SessionState.CONNECTED && session.getState() != SessionState.TRANSFERRING) {
            return Optional.of(TransferResult.failure("Player not connected"));
        }

        if (session.getPlayerUuid() == null) {
            return Optional.of(TransferResult.failure("Player UUID not known"));
        }

        BackendServer current = session.getCurrentBackend();
        if (current != null && current.getName().equalsIgnoreCase(targetBackend.getName())) {
            return Optional.of(TransferResult.failure("Already connected to this server"));
        }

        return Optional.empty();
    }

    // ==================== Address Resolution ====================

    private HostAddress resolveProxyAddress(ProxySession session) {
        String host = resolveProxyHost();
        int port = resolveProxyPort();

        if (port > MAX_PORT) {
            LOGGER.error("Session {}: Port {} exceeds maximum ({}) for ClientReferral", session.getSessionId(), port, MAX_PORT);
            return null;
        }

        return new HostAddress(host, (short) port);
    }

    private String resolveProxyHost() {
        String publicAddr = proxyCore.getConfig().getPublicAddress();
        if (isValidHost(publicAddr)) {
            return publicAddr;
        }

        String bindAddr = proxyCore.getConfig().getBindAddress();
        if (isValidHost(bindAddr)) {
            return bindAddr;
        }

        LOGGER.warn("No publicAddress configured - using localhost");
        return "127.0.0.1";
    }

    private boolean isValidHost(String host) {
        return host != null && !host.isEmpty() && !"0.0.0.0".equals(host);
    }

    private int resolveProxyPort() {
        int publicPort = proxyCore.getConfig().getPublicPort();
        return publicPort > 0 ? publicPort : proxyCore.getConfig().getBindPort();
    }

    // ==================== Referral Handling ====================

    private void logTransferStart(ProxySession session, BackendServer targetBackend) {
        BackendServer current = session.getCurrentBackend();
        LOGGER.info("Session {}: Initiating transfer for {} from {} to {}", session.getSessionId(), session.getPlayerName(), current != null ? current.getName() : "unknown", targetBackend.getName());
    }

    private byte[] createReferralData(ProxySession session, BackendServer targetBackend) {
        // Ensure ProxyCore has this getter!
        return proxyCore.getReferralManager().createReferral(session.getPlayerUuid(), targetBackend);
    }

    private void sendClientReferral(ProxySession session, HostAddress address, byte[] referralData) {
        ClientReferral referral = new ClientReferral(address, referralData);

        LOGGER.info("Session {}: Sending ClientReferral to {} -> {}:{}", session.getSessionId(), session.getPlayerName(), address.host, address.port);

        session.sendToClient(referral);
    }
}