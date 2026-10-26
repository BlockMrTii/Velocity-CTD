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

package com.velocitypowered.proxy.connection.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.connection.ConnectionTypes;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.client.ClientConnectionPhases;
import com.velocitypowered.proxy.connection.client.ClientPlaySessionHandler;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.connection.player.bossbar.BossBarManager;
import com.velocitypowered.proxy.connection.registry.DimensionInfo;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.chat.ComponentHolder;
import com.velocitypowered.proxy.server.VelocityRegisteredServer;
import com.velocitypowered.proxy.tablist.InternalTabList;
import com.velocitypowered.proxy.util.VelocityChannelRegistrar;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Reproduces packets decoded behind JoinGame before the backend play observer is installed. */
class ScoreboardRoundTripTest {

  private static final ProtocolVersion VERSION = ProtocolVersion.MINECRAFT_26_3;
  private final List<ByteBuf> forwarded = new ArrayList<>();
  private final List<EmbeddedChannel> channels = new ArrayList<>();
  private final Set<String> objectives = new HashSet<>();
  private final Set<String> teams = new HashSet<>();
  private VelocityServer server;
  private ConnectedPlayer player;
  private MinecraftConnection client;
  private ClientPlaySessionHandler play;
  private VelocityConfiguration config;

  @BeforeEach
  void setUp() {
    server = mock(VelocityServer.class);
    config = mock(VelocityConfiguration.class);
    when(server.getConfiguration()).thenReturn(config);
    when(config.isSkipConfigOnSwitch()).thenReturn(true);
    when(server.getChannelRegistrar()).thenReturn(new VelocityChannelRegistrar());
    player = mock(ConnectedPlayer.class);
    when(player.getProtocolVersion()).thenReturn(VERSION);
    when(player.getTabList()).thenReturn(mock(InternalTabList.class));
    when(player.getPhase()).thenReturn(ClientConnectionPhases.VANILLA);
    when(player.getBossBarManager()).thenReturn(mock(BossBarManager.class));
    client = connection(true);
    when(client.getType()).thenReturn(ConnectionTypes.VANILLA);
    when(player.getConnection()).thenReturn(client);
    play = new ClientPlaySessionHandler(server, player);
    when(client.getActiveSessionHandler()).thenReturn(play);
  }

  private MinecraftConnection connection(boolean clientbound) {
    MinecraftConnection connection = mock(MinecraftConnection.class);
    when(connection.getProtocolVersion()).thenReturn(VERSION);
    MinecraftEncoder encoder = new MinecraftEncoder(
        clientbound ? ProtocolUtils.Direction.CLIENTBOUND : ProtocolUtils.Direction.SERVERBOUND);
    encoder.setState(StateRegistry.PLAY);
    encoder.setProtocolVersion(VERSION);
    MinecraftDecoder decoder = new MinecraftDecoder(ProtocolUtils.Direction.CLIENTBOUND);
    decoder.setState(StateRegistry.PLAY);
    decoder.setProtocolVersion(VERSION);
    EmbeddedChannel channel = new EmbeddedChannel(encoder, decoder);
    channels.add(channel);
    when(connection.getChannel()).thenReturn(channel);
    doAnswer(call -> {
      Object message = call.getArgument(0);
      if (clientbound && message instanceof ByteBuf) {
        channel.writeOutbound(message);
        ByteBuf frame;
        while ((frame = channel.readOutbound()) != null) {
          forwarded.add(frame);
          applyToClient(frame.duplicate());
        }
      } else if (message instanceof ByteBuf buf) {
        forwarded.add(buf);
      }
      return null;
    }).when(connection).delayedWrite(any());
    return connection;
  }

  private void applyToClient(ByteBuf frame) {
    int id = ProtocolUtils.readVarInt(frame);
    if (id != 0x6C && id != 0x6F) {
      return;
    }
    String name = ProtocolUtils.readString(frame);
    int mode = frame.readUnsignedByte();
    Set<String> entries = id == 0x6C ? objectives : teams;
    if (mode == 0) {
      assertTrue(entries.add(name), "client would reject duplicate creation: " + name);
    } else if (mode == 1) {
      assertTrue(entries.remove(name), "remove exactly the state still on the client: " + name);
    }
  }

  private VelocityServerConnection backend() {
    VelocityServerConnection backend = mock(VelocityServerConnection.class);
    MinecraftConnection connection = connection(false);
    when(backend.getPlayer()).thenReturn(player);
    when(backend.getServer()).thenReturn(mock(VelocityRegisteredServer.class));
    when(backend.ensureConnected()).thenReturn(connection);
    when(backend.getConnection()).thenReturn(connection);
    return backend;
  }

  private JoinGamePacket joinGame() {
    JoinGamePacket join = new JoinGamePacket();
    join.setEntityId(42);
    join.setDimensionInfo(new DimensionInfo("minecraft:overworld", "minecraft:overworld",
        false, false, VERSION));
    return join;
  }

  private void enter(String objective, String team) throws Exception {
    VelocityServerConnection backend = backend();
    TransitionSessionHandler transition = new TransitionSessionHandler(server, backend,
        new CompletableFuture<>());
    // The frame decoder can deliver this batch while JoinGame's server-connected event waits.
    ByteBuf createObjective = scoreboard(0x6C, objective, 0);
    ByteBuf createTeam = scoreboard(0x6F, team, 0);
    transition.handleUnknown(createObjective);
    transition.handleUnknown(createTeam);
    createObjective.release();
    createTeam.release();
    play.handleBackendJoinGame(joinGame(), backend);
    Method flush = TransitionSessionHandler.class.getDeclaredMethod("flushDeferredPackets");
    flush.setAccessible(true);
    flush.invoke(transition);
  }

  private static ByteBuf scoreboard(int id, String name, int mode) {
    ByteBuf frame = Unpooled.buffer();
    ProtocolUtils.writeVarInt(frame, id);
    ProtocolUtils.writeString(frame, name);
    frame.writeByte(mode);
    if (id == 0x6C && mode == 0) {
      new ComponentHolder(VERSION, Component.text(name)).write(frame);
      ProtocolUtils.writeVarInt(frame, 0); // integer render type
      frame.writeBoolean(false); // no custom number format
    }
    return frame;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void duelsLobbyDuelsRemovesBufferedObjectivesAndTeamsBeforeTheyAreRecreated(boolean seamless)
      throws Exception {
    when(config.isSeamlessSwitch()).thenReturn(seamless);
    enter("northduels", "duels-team");
    assertEquals(Set.of("northduels"), objectives);
    assertEquals(Set.of("duels-team"), teams);
    for (int repeat = 0; repeat < 3; repeat++) {
      enter("northlobby", "lobby-team");
      assertFalse(objectives.contains("northduels"), "Duels state must be removed on the lobby hop");
      assertFalse(teams.contains("duels-team"));
      enter("northduels", "duels-team");
      assertEquals(Set.of("northduels"), objectives);
      assertEquals(Set.of("duels-team"), teams);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void objectivesSentDirectlyToClientAreCleanedOnEverySwitch(boolean seamless) throws Exception {
    when(config.isSeamlessSwitch()).thenReturn(seamless);
    play.handleBackendJoinGame(joinGame(), backend());
    // A proxy plugin can write these without passing through the backend decoder or transition.
    for (int repeat = 0; repeat < 3; repeat++) {
      client.delayedWrite(scoreboard(0x6C, "northduels", 0));
      client.delayedWrite(scoreboard(0x6F, "duels-team", 0));
      client.delayedWrite(scoreboard(0x6C, "second-objective", 0));
      client.delayedWrite(scoreboard(0x6F, "second-team", 0));
      enter("northlobby", "lobby-team");
      assertEquals(Set.of("northlobby"), objectives);
      assertEquals(Set.of("lobby-team"), teams);
      play.handleBackendJoinGame(joinGame(), backend());
      assertTrue(objectives.isEmpty());
      assertTrue(teams.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void discardedPacketsFromAnObsoleteBackendCannotEraseClientState(boolean seamless)
      throws Exception {
    when(config.isSeamlessSwitch()).thenReturn(seamless);
    enter("northduels", "duels-team");
    VelocityServerConnection oldBackend = backend();
    BackendPlaySessionHandler handler = new BackendPlaySessionHandler(server, oldBackend);
    handler.activated();
    EmbeddedChannel channel = (EmbeddedChannel) oldBackend.ensureConnected().getChannel();
    // Decoding precedes beforeHandle(), which discards all packets from obsolete connections.
    channel.writeInbound(scoreboard(0x6C, "northduels", 1));
    channel.writeInbound(scoreboard(0x6F, "duels-team", 1));
    assertTrue(handler.beforeHandle());
    ByteBuf discarded;
    while ((discarded = channel.readInbound()) != null) {
      discarded.release();
    }
    enter("northlobby", "lobby-team");
    assertEquals(Set.of("northlobby"), objectives);
    assertEquals(Set.of("lobby-team"), teams);
    enter("northduels", "duels-team");
  }

  @AfterEach
  void releasePackets() {
    channels.forEach(EmbeddedChannel::finishAndReleaseAll);
    forwarded.forEach(buf -> {
      if (buf.refCnt() > 0) {
        buf.release();
      }
    });
  }
}
