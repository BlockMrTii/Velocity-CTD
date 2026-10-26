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

import com.velocitypowered.api.event.player.CookieReceiveEvent;
import com.velocitypowered.api.event.player.CookieRequestEvent;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.protocol.packet.ClientboundCookieRequestPacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundCookieResponsePacket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import net.kyori.adventure.key.Key;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Routes cookies to the connection that requested them, even while the client stays in PLAY. */
final class CookieRequestHandler {

  private final VelocityServer server;
  private final ConnectedPlayer player;
  // Only accessed on the client event loop. Keep disconnected origins until their response arrives:
  // removing one early would give its response to a newer request for the same key.
  private final Map<Key, Queue<Request>> pending = new HashMap<>();

  CookieRequestHandler(VelocityServer server, ConnectedPlayer player) {
    this.server = server;
    this.player = player;
  }

  void request(Key key, @Nullable MinecraftConnection backend) {
    MinecraftConnection client = player.getConnection();
    server.getEventManager().fire(new CookieRequestEvent(player, key))
        .thenAcceptAsync(event -> {
          if (!event.getResult().isAllowed() || client.isClosed()
              || (backend != null && backend.isClosed())) {
            return;
          }
          Key sentKey = event.getResult().getKey() == null ? key : event.getResult().getKey();
          pending.computeIfAbsent(sentKey, ignored -> new ArrayDeque<>())
              .add(new Request(key, backend));
          client.write(new ClientboundCookieRequestPacket(sentKey));
        }, client.eventLoop());
  }

  void receive(ServerboundCookieResponsePacket packet) {
    // Consume before awaiting plugins: a server switch or a second response must not change origin.
    Queue<Request> requests = pending.get(packet.getKey());
    Request request = requests == null ? null : requests.poll();
    if (requests != null && requests.isEmpty()) {
      pending.remove(packet.getKey());
    }
    server.getEventManager()
        .fire(new CookieReceiveEvent(player, packet.getKey(), packet.getPayload()))
        .thenAcceptAsync(event -> {
          // Proxy-originated and unsolicited responses must never be forwarded to a backend.
          if (!event.getResult().isAllowed() || request == null || request.backend() == null) {
            return;
          }
          MinecraftConnection backend = request.backend();
          Key key = event.getResult().getKey() == null ? request.key() : event.getResult().getKey();
          byte[] data = event.getResult().getData() == null
              ? event.getOriginalData() : event.getResult().getData();
          backend.eventLoop().execute(() -> {
            if (!backend.isClosed()) {
              backend.write(new ServerboundCookieResponsePacket(key, data));
            }
          });
        }, player.getConnection().eventLoop());
  }

  private record Request(Key key, @Nullable MinecraftConnection backend) {
  }
}
