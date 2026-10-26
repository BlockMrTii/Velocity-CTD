/*
 * Copyright (C) 2018-2023 Velocity Contributors
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

package com.velocitypowered.proxy.protocol.netty;

import com.google.common.base.Preconditions;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Encodes {@link MinecraftPacket} instances.
 */
public class MinecraftEncoder extends MessageToByteEncoder<MinecraftPacket> {

  private final ProtocolUtils.Direction direction;
  private StateRegistry state;
  private @Nullable Consumer<ByteBuf> packetObserver;
  private StateRegistry.PacketRegistry.ProtocolRegistry registry;

  /**
   * Creates a new {@code MinecraftEncoder} encoding packets for the specified {@code direction}.
   *
   * @param direction the direction to encode to
   */
  public MinecraftEncoder(ProtocolUtils.Direction direction) {
    this.direction = Preconditions.checkNotNull(direction, "direction");
    this.registry = StateRegistry.HANDSHAKE.getProtocolRegistry(
        direction, ProtocolVersion.MINIMUM_VERSION);
    this.state = StateRegistry.HANDSHAKE;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, MinecraftPacket msg, ByteBuf out) {
    int start = out.writerIndex();
    int packetId = this.registry.getPacketId(msg);
    ProtocolUtils.writeVarInt(out, packetId);
    msg.encode(out, direction, registry.version);
    if (packetObserver != null && state == StateRegistry.PLAY) {
      packetObserver.accept(out.duplicate().readerIndex(start));
    }
  }

  /**
   * Observes packets encoded by synchronous writes on this channel's event loop. Raw buffers bypass
   * this encoder and must be observed by the caller. The observer must not modify or retain buffers.
   *
   * @param observer receives each encoded PLAY packet including its ID
   * @param writes the writes to observe
   */
  public void observeEncodedPackets(Consumer<ByteBuf> observer, Runnable writes) {
    Consumer<ByteBuf> previous = packetObserver;
    packetObserver = observer;
    try {
      writes.run();
    } finally {
      packetObserver = previous;
    }
  }

  @Override
  protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, MinecraftPacket msg,
      boolean preferDirect) throws Exception {
    int hint = msg.encodeSizeHint(direction, registry.version);
    if (hint < 0) {
      return super.allocateBuffer(ctx, msg, preferDirect);
    }

    int packetId = this.registry.getPacketId(msg);
    int totalHint = ProtocolUtils.varIntBytes(packetId) + hint;
    return preferDirect ? ctx.alloc().ioBuffer(totalHint) : ctx.alloc().heapBuffer(totalHint);
  }

  public void setProtocolVersion(final ProtocolVersion protocolVersion) {
    this.registry = state.getProtocolRegistry(direction, protocolVersion);
  }

  public void setState(StateRegistry state) {
    this.state = state;
    this.setProtocolVersion(registry.version);
  }

  public ProtocolUtils.Direction getDirection() {
    return direction;
  }
}
