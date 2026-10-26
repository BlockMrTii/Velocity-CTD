/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.event.player.CookieReceiveEvent;
import com.velocitypowered.api.event.player.CookieRequestEvent;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.connection.player.resourcepack.ResourcePackTransfer;
import com.velocitypowered.proxy.event.VelocityEventManager;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import com.velocitypowered.proxy.protocol.packet.ClientboundCookieRequestPacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundCookieResponsePacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.key.Key;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class CookieRequestHandlerTest {

  private static final Key KEY = Key.key("test", "cookie");
  private static final byte[] DATA = {1, 2, 3};
  private final List<EmbeddedChannel> channels = new ArrayList<>();
  private VelocityServer server;
  private VelocityEventManager events;
  private ConnectedPlayer player;
  private MinecraftConnection client;
  private EmbeddedChannel clientChannel;
  private CookieRequestHandler handler;

  @BeforeEach
  void setUp() {
    server = mock(VelocityServer.class);
    events = mock(VelocityEventManager.class);
    when(server.getEventManager()).thenReturn(events);
    when(events.fire(any(CookieRequestEvent.class)))
        .thenAnswer(call -> CompletableFuture.completedFuture(call.getArgument(0)));
    when(events.fire(any(CookieReceiveEvent.class)))
        .thenAnswer(call -> CompletableFuture.completedFuture(call.getArgument(0)));
    player = mock(ConnectedPlayer.class);
    clientChannel = channel();
    client = mock(MinecraftConnection.class);
    when(client.eventLoop()).thenReturn(clientChannel.eventLoop());
    when(player.getConnection()).thenReturn(client);
    when(player.getProtocolVersion()).thenReturn(ProtocolVersion.MINECRAFT_26_3);
    handler = new CookieRequestHandler(server, player);
  }

  private EmbeddedChannel channel() {
    EmbeddedChannel channel = new EmbeddedChannel();
    channels.add(channel);
    return channel;
  }

  private MinecraftConnection backend(StateRegistry state) {
    EmbeddedChannel channel = channel();
    MinecraftEncoder encoder = new MinecraftEncoder(ProtocolUtils.Direction.SERVERBOUND);
    encoder.setState(state);
    encoder.setProtocolVersion(ProtocolVersion.MINECRAFT_26_3);
    channel.pipeline().addLast(encoder);
    MinecraftConnection backend = mock(MinecraftConnection.class);
    when(backend.eventLoop()).thenReturn(channel.eventLoop());
    when(backend.isClosed()).thenAnswer(call -> !channel.isActive());
    when(backend.getChannel()).thenReturn(channel);
    when(backend.write(any())).thenAnswer(call -> {
      Object message = call.getArgument(0);
      channel.writeOutbound(message);
      return null;
    });
    return backend;
  }

  private void request(Key key, MinecraftConnection backend) {
    handler.request(key, backend);
    clientChannel.runPendingTasks();
  }

  private void respond(Key key) {
    handler.receive(new ServerboundCookieResponsePacket(key, DATA));
    clientChannel.runPendingTasks();
    channels.forEach(EmbeddedChannel::runPendingTasks);
  }

  private void assertResponse(MinecraftConnection backend, StateRegistry state, Key key) {
    ByteBuf frame = ((EmbeddedChannel) backend.getChannel()).readOutbound();
    try {
      ServerboundCookieResponsePacket packet = new ServerboundCookieResponsePacket();
      assertEquals(state.getProtocolRegistry(ProtocolUtils.Direction.SERVERBOUND,
          ProtocolVersion.MINECRAFT_26_3).getPacketId(packet), ProtocolUtils.readVarInt(frame));
      packet.decode(frame, ProtocolUtils.Direction.SERVERBOUND, ProtocolVersion.MINECRAFT_26_3);
      assertEquals(key, packet.getKey());
      assertArrayEquals(DATA, packet.getPayload());
      assertEquals(0, frame.readableBytes());
    } finally {
      if (frame != null) {
        frame.release();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StateRegistry.class, names = {"LOGIN", "CONFIG", "PLAY"})
  void playClientAnswersTheRequestingBackendInItsOwnProtocol(StateRegistry state) {
    MinecraftConnection lobby = backend(StateRegistry.PLAY);
    MinecraftConnection target = backend(state);
    // While the new backend configures, the player is still connected to the old lobby.
    VelocityServerConnection connected = mock(VelocityServerConnection.class);
    when(player.getConnectedServer()).thenReturn(connected);
    when(connected.getConnection()).thenReturn(lobby);

    request(KEY, target);
    respond(KEY);

    assertResponse(target, state, KEY);
    verify(lobby, never()).write(any());
  }

  @Test
  void lateLobbyReplyDoesNotReachTheNewServer() {
    MinecraftConnection lobby = backend(StateRegistry.PLAY);
    MinecraftConnection target = backend(StateRegistry.PLAY);
    request(KEY, lobby);
    ((EmbeddedChannel) lobby.getChannel()).close();
    request(KEY, target);

    respond(KEY);
    verify(target, never()).write(any());
    respond(KEY);
    assertResponse(target, StateRegistry.PLAY, KEY);
  }

  @Test
  void unsolicitedAndDuplicateResponsesAreNotForwarded() {
    MinecraftConnection backend = backend(StateRegistry.PLAY);
    respond(KEY);
    verify(backend, never()).write(any());
    request(KEY, backend);
    respond(KEY);
    assertResponse(backend, StateRegistry.PLAY, KEY);
    respond(KEY);
    assertNull(((EmbeddedChannel) backend.getChannel()).readOutbound());
  }

  @Test
  void proxyCookieDoesNotConsumeBackendRequestForTheSameKey() {
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, null);
    request(KEY, backend);
    respond(KEY);
    verify(backend, never()).write(any());
    respond(KEY);
    assertResponse(backend, StateRegistry.CONFIG, KEY);
  }

  @Test
  void differentKeysCanBeAnsweredOutOfOrder() {
    Key other = Key.key("test", "other");
    MinecraftConnection lobby = backend(StateRegistry.PLAY);
    MinecraftConnection target = backend(StateRegistry.CONFIG);
    request(KEY, lobby);
    request(other, target);
    respond(other);
    assertResponse(target, StateRegistry.CONFIG, other);
    respond(KEY);
    assertResponse(lobby, StateRegistry.PLAY, KEY);
  }

  @Test
  void requestKeyRewritingStillReturnsTheBackendKey() {
    Key rewritten = Key.key("proxy", "rewritten");
    when(events.fire(any(CookieRequestEvent.class))).thenAnswer(call -> {
      CookieRequestEvent event = call.getArgument(0);
      event.setResult(CookieRequestEvent.ForwardResult.key(rewritten));
      return CompletableFuture.completedFuture(event);
    });
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, backend);
    ArgumentCaptor<ClientboundCookieRequestPacket> sent =
        ArgumentCaptor.forClass(ClientboundCookieRequestPacket.class);
    verify(client).write(sent.capture());
    assertEquals(rewritten, sent.getValue().getKey());
    respond(rewritten);
    assertResponse(backend, StateRegistry.CONFIG, KEY);
  }

  @Test
  void deniedRequestsAreNotSentOrTracked() {
    when(events.fire(any(CookieRequestEvent.class))).thenAnswer(call -> {
      CookieRequestEvent event = call.getArgument(0);
      event.setResult(CookieRequestEvent.ForwardResult.handled());
      return CompletableFuture.completedFuture(event);
    });
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, backend);
    respond(KEY);
    verify(client, never()).write(any());
    verify(backend, never()).write(any());
  }

  @Test
  void handledResponsesStillConsumeTheirRequest() {
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, backend);
    when(events.fire(any(CookieReceiveEvent.class))).thenAnswer(call -> {
      CookieReceiveEvent event = call.getArgument(0);
      event.setResult(CookieReceiveEvent.ForwardResult.handled());
      return CompletableFuture.completedFuture(event);
    });
    respond(KEY);
    verify(backend, never()).write(any());
    when(events.fire(any(CookieReceiveEvent.class)))
        .thenAnswer(call -> CompletableFuture.completedFuture(call.getArgument(0)));
    respond(KEY);
    verify(backend, never()).write(any());
  }

  @Test
  void backendDisconnectWhileReceiveEventWaitsDoesNotRedirectTheResponse() {
    MinecraftConnection origin = backend(StateRegistry.PLAY);
    request(KEY, origin);
    CompletableFuture<CookieReceiveEvent> pending = new CompletableFuture<>();
    when(events.fire(any(CookieReceiveEvent.class))).thenReturn(pending);
    handler.receive(new ServerboundCookieResponsePacket(KEY, DATA));
    ((EmbeddedChannel) origin.getChannel()).close();
    pending.complete(new CookieReceiveEvent(player, KEY, DATA));
    clientChannel.runPendingTasks();
    channels.forEach(EmbeddedChannel::runPendingTasks);
    verify(origin, never()).write(any());
  }

  @Test
  void backendDisconnectWhileRequestEventWaitsDoesNotSendStaleQuery() {
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    CompletableFuture<CookieRequestEvent> pending = new CompletableFuture<>();
    when(events.fire(any(CookieRequestEvent.class))).thenReturn(pending);
    handler.request(KEY, backend);
    ((EmbeddedChannel) backend.getChannel()).close();
    pending.complete(new CookieRequestEvent(player, KEY));
    clientChannel.runPendingTasks();
    verify(client, never()).write(any());
  }

  @Test
  void receiveEventCanRewriteTheResponseKey() {
    Key rewritten = Key.key("test", "modified");
    when(events.fire(any(CookieReceiveEvent.class))).thenAnswer(call -> {
      CookieReceiveEvent event = call.getArgument(0);
      event.setResult(CookieReceiveEvent.ForwardResult.key(rewritten));
      return CompletableFuture.completedFuture(event);
    });
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, backend);
    respond(KEY);
    assertResponse(backend, StateRegistry.CONFIG, rewritten);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void bothClientStatesUseTheSameCookieRequestHistory(boolean play) {
    ServerboundCookieResponsePacket response = new ServerboundCookieResponsePacket(KEY, DATA);
    if (play) {
      new ClientPlaySessionHandler(server, player).handle(response);
    } else {
      new ClientConfigSessionHandler(server, player).handle(response);
    }
    verify(player).handleCookieResponse(response);
  }

  private AuthSessionHandler authHandler(CompletableFuture<byte[]> resourcePacks) {
    VelocityConfiguration config = mock(VelocityConfiguration.class);
    when(server.getConfiguration()).thenReturn(config);
    when(config.getMaximumVersion()).thenReturn(Optional.empty());
    LoginInboundConnection inbound = mock(LoginInboundConnection.class);
    when(inbound.delegatedConnection()).thenReturn(client);
    return new AuthSessionHandler(server, inbound,
        new GameProfile(UUID.randomUUID(), "Player", List.of()), false, null, resourcePacks);
  }

  @Test
  void authenticationUsesTheSameRequestHistory() throws ReflectiveOperationException {
    AuthSessionHandler auth = authHandler(new CompletableFuture<>());
    Field connected = AuthSessionHandler.class.getDeclaredField("connectedPlayer");
    connected.setAccessible(true);
    connected.set(auth, player);
    ServerboundCookieResponsePacket response = new ServerboundCookieResponsePacket(KEY, DATA);
    auth.handle(response);
    verify(player).handleCookieResponse(response);
  }

  @Test
  void authenticationResourcePackCookieStillCompletesItsOwnFuture() {
    CompletableFuture<byte[]> resourcePacks = new CompletableFuture<>();
    AuthSessionHandler auth = authHandler(resourcePacks);
    auth.handle(new ServerboundCookieResponsePacket(
        ResourcePackTransfer.APPLIED_RESOURCE_PACKS_KEY, DATA));
    assertArrayEquals(DATA, resourcePacks.join());
    verify(player, never()).handleCookieResponse(any());
  }

  @Test
  void authenticationRejectsOtherCookiesBeforeThePlayerExists() {
    authHandler(new CompletableFuture<>())
        .handle(new ServerboundCookieResponsePacket(KEY, DATA));
    verify(client).close(true);
  }

  @Test
  void absentCookiePayloadIsPreserved() {
    MinecraftConnection backend = backend(StateRegistry.CONFIG);
    request(KEY, backend);
    handler.receive(new ServerboundCookieResponsePacket(KEY, null));
    clientChannel.runPendingTasks();
    channels.forEach(EmbeddedChannel::runPendingTasks);
    ByteBuf frame = ((EmbeddedChannel) backend.getChannel()).readOutbound();
    try {
      ProtocolUtils.readVarInt(frame);
      ServerboundCookieResponsePacket response = new ServerboundCookieResponsePacket();
      response.decode(frame, ProtocolUtils.Direction.SERVERBOUND, ProtocolVersion.MINECRAFT_26_3);
      assertEquals(KEY, response.getKey());
      assertNull(response.getPayload());
    } finally {
      if (frame != null) {
        frame.release();
      }
    }
  }

  @AfterEach
  void tearDown() {
    channels.forEach(EmbeddedChannel::finishAndReleaseAll);
  }
}
