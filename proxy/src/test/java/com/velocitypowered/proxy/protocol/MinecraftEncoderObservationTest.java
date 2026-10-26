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

package com.velocitypowered.proxy.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import com.velocitypowered.proxy.protocol.packet.ClientboundCookieRequestPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import net.kyori.adventure.key.Key;
import org.junit.jupiter.api.Test;

class MinecraftEncoderObservationTest {

  private static final ProtocolVersion VERSION = ProtocolVersion.MINECRAFT_26_3;

  private MinecraftEncoder encoder() throws ReflectiveOperationException {
    // Model a plugin registering a typed scoreboard packet, without changing the global registry.
    StateRegistry.PacketRegistry registry = new StateRegistry.PacketRegistry(
        ProtocolUtils.Direction.CLIENTBOUND, StateRegistry.PLAY);
    registry.register(ObjectivePacket.class, ObjectivePacket::new,
        new StateRegistry.PacketMapping(0x6C, VERSION, null, false));
    MinecraftEncoder encoder = new MinecraftEncoder(ProtocolUtils.Direction.CLIENTBOUND);
    encoder.setState(StateRegistry.PLAY);
    Field field = MinecraftEncoder.class.getDeclaredField("registry");
    field.setAccessible(true);
    field.set(encoder, registry.getProtocolRegistry(VERSION));
    return encoder;
  }

  @Test
  void pluginDecodedObjectiveIsObservedWithoutEncodingItTwiceOrConsumingTheForwardedBytes()
      throws Exception {
    MinecraftEncoder encoder = encoder();
    EmbeddedChannel channel = new EmbeddedChannel(encoder);
    ObjectivePacket packet = new ObjectivePacket();
    List<String> seen = new ArrayList<>();
    try {
      encoder.setPlayPacketObserver(frame -> {
        assertEquals(0x6C, ProtocolUtils.readVarInt(frame));
        seen.add(ProtocolUtils.readString(frame));
        assertEquals(0, frame.readUnsignedByte());
      });
      channel.writeOutbound(packet);
      assertEquals(1, packet.encodes, "observation must use the real encoding");
      assertEquals(List.of("northduels"), seen);
      ByteBuf forwarded = channel.readOutbound();
      try {
        assertEquals(0x6C, ProtocolUtils.readVarInt(forwarded));
        assertEquals("northduels", ProtocolUtils.readString(forwarded));
        assertEquals(0, forwarded.readUnsignedByte());
      } finally {
        forwarded.release();
      }
      channel.writeOutbound(new ObjectivePacket());
      assertEquals(2, seen.size(), "later PLAY writes must also be tracked");
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void rawPacketsAreObservedExactlyOnce() throws Exception {
    MinecraftEncoder encoder = encoder();
    EmbeddedChannel channel = new EmbeddedChannel(encoder);
    List<String> seen = new ArrayList<>();
    try {
      encoder.setPlayPacketObserver(frame -> seen.add("raw"));
      channel.writeOutbound(Unpooled.buffer().writeByte(0x6C));
      assertEquals(List.of("raw"), seen);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void observerCanBeDisabledWhenTheClientHandlerChanges() throws Exception {
    MinecraftEncoder encoder = encoder();
    EmbeddedChannel channel = new EmbeddedChannel(encoder);
    List<String> seen = new ArrayList<>();
    try {
      encoder.setPlayPacketObserver(frame -> seen.add("tracked"));
      channel.writeOutbound(new ObjectivePacket());
      encoder.setPlayPacketObserver(null);
      channel.writeOutbound(new ObjectivePacket());
      channel.writeOutbound(Unpooled.buffer().writeByte(0x6C));
      assertEquals(List.of("tracked"), seen);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void configurationPacketIdsAreNeverInterpretedAsPlayWorldState() {
    MinecraftEncoder encoder = new MinecraftEncoder(ProtocolUtils.Direction.CLIENTBOUND);
    encoder.setState(StateRegistry.CONFIG);
    encoder.setProtocolVersion(VERSION);
    EmbeddedChannel channel = new EmbeddedChannel(encoder);
    List<String> seen = new ArrayList<>();
    try {
      encoder.setPlayPacketObserver(frame -> seen.add("unexpected"));
      channel.writeOutbound(new ClientboundCookieRequestPacket(Key.key("test", "cookie")));
      channel.writeOutbound(Unpooled.buffer().writeByte(0x6C));
      assertEquals(List.of(), seen);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  private static final class ObjectivePacket implements MinecraftPacket {

    private int encodes;

    @Override
    public void decode(ByteBuf buf, ProtocolUtils.Direction direction, ProtocolVersion version) {
    }

    @Override
    public void encode(ByteBuf buf, ProtocolUtils.Direction direction, ProtocolVersion version) {
      encodes++;
      ProtocolUtils.writeString(buf, "northduels");
      buf.writeByte(0);
    }

    @Override
    public boolean handle(MinecraftSessionHandler handler) {
      return false;
    }
  }
}
