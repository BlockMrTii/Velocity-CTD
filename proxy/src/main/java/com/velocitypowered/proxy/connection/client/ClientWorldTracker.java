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

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Tracks what the current backend has put into the client's level (entities, chunks, the player's
 * effects), so a server switch into the same level can clean it up without JoinGame or Respawn.
 * Both of those make the client open the "Loading terrain" screen.
 *
 * <p>All calls happen on the player's event loop, which backend connections share.
 */
public final class ClientWorldTracker {

  // ponytail: packet IDs only for 26.3 (taken from the client's GameProtocols). Other versions use
  // the normal JoinGame switch; add their IDs here to support them.
  private static final int ADD_ENTITY = 0x01;
  private static final int CONTAINER_CLOSE = 0x11;
  private static final int FORGET_LEVEL_CHUNK = 0x26;
  private static final int GAME_EVENT = 0x27;
  private static final int LEVEL_CHUNK_WITH_LIGHT = 0x2E;
  private static final int REMOVE_ENTITIES = 0x4E;
  private static final int REMOVE_MOB_EFFECT = 0x4F;
  private static final int SET_CHUNK_CACHE_RADIUS = 0x61;
  private static final int SET_OBJECTIVE = 0x6C;
  private static final int SET_PLAYER_TEAM = 0x6F;
  private static final int SET_SIMULATION_DISTANCE = 0x71;
  private static final int STOP_SOUND = 0x79;
  private static final int UPDATE_MOB_EFFECT = 0x87;
  private static final int SERVERBOUND_PLAYER_LOADED = 0x2C;

  private static final int METHOD_ADD = 0;
  private static final int METHOD_REMOVE = 1;

  private static final int EVENT_STOP_RAINING = 2;
  private static final int EVENT_CHANGE_GAME_MODE = 3;
  private static final int EVENT_RAIN_LEVEL = 7;
  private static final int EVENT_THUNDER_LEVEL = 8;

  private final IntSet entities = new IntOpenHashSet();
  private final LongSet chunks = new LongOpenHashSet();
  private final IntSet playerEffects = new IntOpenHashSet();
  // The scoreboard lives in the client's connection, not its level, so not even JoinGame clears it.
  // Normally the config phase does; a duplicate objective or team disconnects the client.
  private final Set<String> objectives = new HashSet<>();
  private final Set<String> teams = new HashSet<>();
  private int playerEntityId;

  public static boolean supports(ProtocolVersion version) {
    return version == ProtocolVersion.MINECRAFT_26_3;
  }

  int getPlayerEntityId() {
    return playerEntityId;
  }

  /**
   * Called whenever the client received a JoinGame, which gives it a fresh level.
   *
   * @param playerEntityId the player's entity ID from that JoinGame
   */
  void reset(int playerEntityId) {
    this.playerEntityId = playerEntityId;
    entities.clear();
    chunks.clear();
    playerEffects.clear();
  }

  /**
   * Inspects a clientbound packet that Velocity does not decode itself. The sets may contain IDs
   * that are already gone on the client (e.g. after a dimension change); removing those again on
   * a switch is harmless.
   *
   * @param buf the packet, starting with its ID; it is not modified
   */
  public void onClientbound(ByteBuf buf) {
    ByteBuf in = buf.duplicate();
    switch (ProtocolUtils.readVarInt(in)) {
      case ADD_ENTITY -> entities.add(ProtocolUtils.readVarInt(in));
      case REMOVE_ENTITIES -> {
        int count = ProtocolUtils.readVarInt(in);
        for (int i = 0; i < count; i++) {
          entities.remove(ProtocolUtils.readVarInt(in));
        }
      }
      case LEVEL_CHUNK_WITH_LIGHT -> {
        int x = in.readInt();
        int z = in.readInt();
        chunks.add(chunkPos(x, z));
      }
      case FORGET_LEVEL_CHUNK -> chunks.remove(in.readLong());
      case SET_OBJECTIVE -> track(objectives, ProtocolUtils.readString(in), in.readByte());
      case SET_PLAYER_TEAM -> track(teams, ProtocolUtils.readString(in), in.readByte());
      case UPDATE_MOB_EFFECT -> {
        if (ProtocolUtils.readVarInt(in) == playerEntityId) {
          playerEffects.add(ProtocolUtils.readVarInt(in));
        }
      }
      case REMOVE_MOB_EFFECT -> {
        if (ProtocolUtils.readVarInt(in) == playerEntityId) {
          playerEffects.remove(ProtocolUtils.readVarInt(in));
        }
      }
      default -> {
      }
    }
  }

  /**
   * Clears what the previous server left on the client and applies what the new server's JoinGame
   * would have set, as far as the new server does not send it again by itself.
   *
   * @param client the client connection
   * @param backend the new backend connection
   * @param joinGame the new backend's JoinGame, which is not sent to the client
   * @param forgetChunks whether to remove the old server's chunks from the client
   */
  void switchServer(MinecraftConnection client, MinecraftConnection backend, JoinGamePacket joinGame,
      boolean forgetChunks) {
    entities.remove(playerEntityId);
    if (!entities.isEmpty()) {
      ByteBuf buf = packet(REMOVE_ENTITIES);
      ProtocolUtils.writeVarInt(buf, entities.size());
      entities.forEach(id -> ProtocolUtils.writeVarInt(buf, id));
      client.delayedWrite(buf);
      entities.clear();
    }
    playerEffects.forEach(effect -> {
      ByteBuf buf = packet(REMOVE_MOB_EFFECT);
      ProtocolUtils.writeVarInt(buf, playerEntityId);
      ProtocolUtils.writeVarInt(buf, effect);
      client.delayedWrite(buf);
    });
    playerEffects.clear();
    // Forget the old terrain right away, so the new server's chunks load into an empty level
    // instead of visibly replacing the old ones.
    if (forgetChunks) {
      chunks.forEach(pos -> client.delayedWrite(packet(FORGET_LEVEL_CHUNK).writeLong(pos)));
    }
    chunks.clear();
    clearScoreboard(client);

    // A menu of the old server (e.g. a server selector) would stay open; the ID is ignored.
    ByteBuf close = packet(CONTAINER_CLOSE);
    ProtocolUtils.writeVarInt(close, 0);
    client.delayedWrite(close);
    client.delayedWrite(packet(STOP_SOUND).writeByte(0)); // no flags: stop all sounds

    // The new server only sends weather if it is raining there.
    client.delayedWrite(gameEvent(EVENT_STOP_RAINING, 0));
    client.delayedWrite(gameEvent(EVENT_RAIN_LEVEL, 0));
    client.delayedWrite(gameEvent(EVENT_THUNDER_LEVEL, 0));
    // These are only part of JoinGame.
    client.delayedWrite(gameEvent(EVENT_CHANGE_GAME_MODE, joinGame.getGamemode()));
    ByteBuf radius = packet(SET_CHUNK_CACHE_RADIUS);
    ProtocolUtils.writeVarInt(radius, joinGame.getViewDistance());
    client.delayedWrite(radius);
    ByteBuf simulation = packet(SET_SIMULATION_DISTANCE);
    ProtocolUtils.writeVarInt(simulation, joinGame.getSimulationDistance());
    client.delayedWrite(simulation);

    // The client only reports this after JoinGame or Respawn. Without it the server treats the
    // player as still loading for a few seconds.
    backend.delayedWrite(packet(SERVERBOUND_PLAYER_LOADED));
  }

  /**
   * Removes the current server's objectives and teams. Needed on every switch that skips the config
   * phase, also when JoinGame is sent, since the scoreboard survives JoinGame.
   *
   * @param client the client connection
   */
  void clearScoreboard(MinecraftConnection client) {
    objectives.forEach(name -> client.delayedWrite(removal(SET_OBJECTIVE, name)));
    objectives.clear();
    teams.forEach(name -> client.delayedWrite(removal(SET_PLAYER_TEAM, name)));
    teams.clear();
  }

  private static void track(Set<String> names, String name, byte method) {
    if (method == METHOD_ADD) {
      names.add(name);
    } else if (method == METHOD_REMOVE) {
      names.remove(name);
    }
  }

  private static ByteBuf removal(int packetId, String name) {
    ByteBuf buf = packet(packetId);
    ProtocolUtils.writeString(buf, name);
    return buf.writeByte(METHOD_REMOVE);
  }

  private static long chunkPos(int x, int z) {
    return (x & 0xFFFFFFFFL) | ((z & 0xFFFFFFFFL) << 32);
  }

  private static ByteBuf gameEvent(int event, float param) {
    return packet(GAME_EVENT).writeByte(event).writeFloat(param);
  }

  private static ByteBuf packet(int id) {
    ByteBuf buf = Unpooled.buffer();
    ProtocolUtils.writeVarInt(buf, id);
    return buf;
  }
}
