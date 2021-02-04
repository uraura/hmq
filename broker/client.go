package broker

import (
	"context"
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
	"net"
	"sync"
)

const (
	// CLIENT is an end user.
	CLIENT = 0
)

const (
	Connected    = 1
	Disconnected = 2
)

type ClientIdentifier string

type client struct {
	id ClientIdentifier

	publishOnly bool // never subscribe messages from broker

	mu         sync.Mutex
	broker     *Broker
	conn       net.Conn
	status     int
	ctx        context.Context
	cancelFunc context.CancelFunc
}

var (
	// frequently used Fixed packets
	DisconnectedPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
	PingrespPacket     = packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
)

func newClient(conn net.Conn, b *Broker, id string) *client {
	ctx, cancel := context.WithCancel(context.Background())

	return &client{
		id:         ClientIdentifier(id),
		broker:     b,
		conn:       conn,
		status:     Connected,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (c *client) loop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			packet, err := packets.ReadPacket(c.conn)
			if err != nil {
				log.Error("read packet error: ", zap.Error(err), zap.Any("ClientID", c.id))
				msg := &Message{
					client: c,
					packet: DisconnectedPacket,
				}
				c.broker.SubmitWork(msg)
				return
			}

			msg := &Message{
				client: c,
				packet: packet,
			}
			c.broker.SubmitWork(msg)
		}
	}
}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}

	switch packet := ca.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
		c.ProcessSubscribe(packet)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.ProcessPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.Close()
	default:
		log.Info("Recv Unknown message.......", zap.Any("ClientID", c.id))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	c.broker.EachClient(func(other *client) error {
		if c.id == other.id || other.publishOnly {
			// skip
			return nil
		}

		// TODO: do something before send

		return other.Send(packet)
	})
}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for range topics {
		// QoS=0 only
		retcodes = append(retcodes, 0)
	}

	suback.ReturnCodes = retcodes

	err := c.Send(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.Any("ClientID", c.id))
		return
	}
}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	err := c.Send(PingrespPacket)
	if err != nil {
		log.Error("send PingResponse error, ", zap.Error(err), zap.Any("ClientID", c.id))
		return
	}
}

func (c *client) Close() {
	if c.status == Disconnected {
		return
	}

	c.cancelFunc()

	c.status = Disconnected
	//wait for message complete
	// time.Sleep(1 * time.Second)
	// c.status = Disconnected

	b := c.broker

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	if b != nil {
		b.DeleteClient(c)
	}
}

func (c *client) Send(packet packets.ControlPacket) error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("recover error, ", zap.Any("recover", err))
		}
	}()
	if c.status == Disconnected {
		return nil
	}

	if packet == nil {
		return nil
	}
	if c.conn == nil {
		c.Close()
		return errors.New("connection lost")
	}

	c.mu.Lock()
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
