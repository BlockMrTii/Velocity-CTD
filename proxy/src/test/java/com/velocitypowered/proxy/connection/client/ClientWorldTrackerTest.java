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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ClientWorldTrackerTest {

  @Test
  void switchRemovesTrackedEntitiesEffectsChunksAndObjectives() {
    ClientWorldTracker tracker = new ClientWorldTracker();
    tracker.reset(42);
    MinecraftConnection client = mock(MinecraftConnection.class);

    tracker.onClientbound(varInts(0x01, 5)); // add entity 5
    tracker.onClientbound(varInts(0x01, 6)); // add entity 6
    tracker.onClientbound(varInts(0x4E, 1, 6)); // remove entity 6
    tracker.onClientbound(varInts(0x87, 42, 3)); // player gets effect 3
    tracker.onClientbound(varInts(0x87, 5, 4)); // another entity gets effect 4
    ByteBuf chunk = varInts(0x2E).writeInt(-1).writeInt(2);
    tracker.onClientbound(chunk);
    assertEquals(0, chunk.readerIndex()); // the forwarded packet is left untouched

    ByteBuf objective = varInts(0x6C);
    ProtocolUtils.writeString(objective, "lobby");
    tracker.onClientbound(objective.writeByte(0)); // add objective "lobby"

    tracker.switchServer(client, mock(MinecraftConnection.class), new JoinGamePacket(), true);
    List<ByteBuf> sent = written(client);
    assertEquals(List.of(0x4E, 1, 5), readVarInts(sent.get(0), 3));
    assertEquals(List.of(0x4F, 42, 3), readVarInts(sent.get(1), 3));
    ByteBuf removeObjective = sent.get(3);
    assertEquals(0x6C, ProtocolUtils.readVarInt(removeObjective));
    assertEquals("lobby", ProtocolUtils.readString(removeObjective));
    assertEquals(1, removeObjective.readByte());

    ByteBuf forget = sent.get(2);
    assertEquals(0x26, ProtocolUtils.readVarInt(forget));
    long pos = forget.readLong();
    assertEquals(-1, (int) pos);
    assertEquals(2, (int) (pos >>> 32));
  }

  @Test
  void switchKeepsChunksWhenDisabled() {
    ClientWorldTracker tracker = new ClientWorldTracker();
    tracker.reset(42);
    MinecraftConnection client = mock(MinecraftConnection.class);
    tracker.onClientbound(varInts(0x2E).writeInt(-1).writeInt(2));

    tracker.switchServer(client, mock(MinecraftConnection.class), new JoinGamePacket(), false);
    for (ByteBuf buf : written(client)) {
      assertNotEquals(0x26, ProtocolUtils.readVarInt(buf));
    }
  }

  private static List<ByteBuf> written(MinecraftConnection client) {
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(client, atLeastOnce()).delayedWrite(captor.capture());
    List<ByteBuf> bufs = new ArrayList<>();
    for (Object o : captor.getAllValues()) {
      bufs.add(((ByteBuf) o).duplicate());
    }
    return bufs;
  }

  private static ByteBuf varInts(int... values) {
    ByteBuf buf = Unpooled.buffer();
    for (int value : values) {
      ProtocolUtils.writeVarInt(buf, value);
    }
    return buf;
  }

  private static List<Integer> readVarInts(ByteBuf buf, int count) {
    List<Integer> values = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      values.add(ProtocolUtils.readVarInt(buf));
    }
    return values;
  }
}
