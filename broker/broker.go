package broker

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/pool"
	"go.uber.org/zap"
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type Broker struct {
	id      string
	mu      sync.Mutex
	config  *Config
	wpool   *pool.WorkerPool
	clients sync.Map
}

func NewBroker(config *Config) (*Broker, error) {
	if config == nil {
		config = DefaultConfig
	}

	b := &Broker{
		id:     GenUniqueId(),
		config: config,
		wpool:  pool.New(config.Worker),
	}

	return b, nil
}

func (b *Broker) AddClient(c *client) {
	b.clients.Store(c.id, c)
}

func (b *Broker) LoadClient(id ClientIdentifier) (*client, bool) {
	if c, found := b.clients.Load(id); found {
		return c.(*client), true
	}

	return nil, false
}

func (b *Broker) DeleteClient(c *client) {
	b.clients.Delete(c.id)
}

func (b *Broker) EachClient(fn func(client *client) error) {
	b.clients.Range(func(_, value interface{}) bool {
		client := value.(*client)

		if err := fn(client); err != nil {
			log.Error("process", zap.Error(err))
		}

		return true // next
	})
}

func (b *Broker) SubmitWork(msg *Message) {
	b.wpool.Submit(string(msg.client.id), func() {
		ProcessMessage(msg)
	})
}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}

	//listen client over tcp
	if b.config.Port != "" {
		go b.StartClientListening()
	}
}

func (b *Broker) StartClientListening() {
	var err error
	var l net.Listener
	// Retry listening indefinitely so that specifying IP addresses
	// (e.g. --host=10.0.0.217) starts working once the IP address is actually
	// configured on the interface.
	for {
		hp := b.config.Host + ":" + b.config.Port
		l, err = net.Listen("tcp", hp)
		log.Info("Start Listening client on ", zap.String("hp", hp))

		if err != nil {
			log.Error("Error listening on ", zap.Error(err))
			time.Sleep(1 * time.Second)
		} else {
			break // successfully listening
		}
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP

	// server main loop
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne), zap.Duration("sleeping", tmpDelay/time.Millisecond))
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", zap.Error(err))
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		go b.handleConnection(conn)

	}
}

func (b *Broker) handleConnectPacket(conn net.Conn) (*packets.ConnectPacket, error) {
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		return nil, err
	}

	if packet == nil {
		return nil, errors.New("nil packet")
	}

	connect, ok := packet.(*packets.ConnectPacket)
	if !ok {
		return nil, errors.New("not connect")
	}

	log.Info("packet", zap.String("connect", connect.String()))
	// CONNECT: dup: false qos: 0 retain: false rLength: 70
	// protocolversion: 4 protocolname: MQTT cleansession: true willflag: false WillQos: 0 WillRetain: false
	// Usernameflag: true Passwordflag: true keepalive: 10 clientId: 3bf74bf7example88fac027
	// willtopic:  willmessage:  Username: xxx_username Password: xxx_password

	return connect, nil
}

func (b *Broker) handleConnackPacket(conn net.Conn, connect *packets.ConnectPacket) error {
	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	// If the Server accepts a connection with CleanSession set to 1,
	// the Server MUST set Session Present to 0 in the CONNACK packet
	// in addition to setting a zero return code in the CONNACK packet [MQTT-3.2.2-1].
	connack.SessionPresent = !connect.CleanSession
	connack.ReturnCode = connect.Validate()

	if connack.ReturnCode != packets.Accepted {
		if err := connack.Write(conn); err != nil {
			return err
		}

		return errors.New("connect not accepted")
	}

	// TODO: Authentication
	//if connect.Username != "xxx" || string(connect.Password) != "xxx" {
	//	connack.ReturnCode = packets.ErrRefusedBadUsernameOrPassword
	//	if err := connack.Write(conn); err != nil {
	//		return err
	//	}
	//
	//	return errors.New("connect not accepted")
	//}

	if _, found := b.LoadClient(ClientIdentifier(connect.ClientIdentifier)); found {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
		if err := connack.Write(conn); err != nil {
			return err
		}
	}

	// success
	if err := connack.Write(conn); err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleConnection(conn net.Conn) {
	// process connect packet
	connect, err := b.handleConnectPacket(conn)
	if err != nil {
		log.Error("process connect", zap.Error(err))
		return
	}

	// process connack packet
	if err := b.handleConnackPacket(conn, connect); err != nil {
		log.Error("process connack", zap.Error(err), zap.String("connect", connect.String()))
		return
	}

	c := newClient(conn, b, connect.ClientIdentifier)

	// save client ids
	b.AddClient(c)

	c.loop()
}
