package redis

import (
	"github.com/azhao1981/go-socket.io"
	"github.com/azhao1981/go-socket.io-redis/cmap_string_cmap"
	"github.com/azhao1981/go-socket.io-redis/cmap_string_socket"
	"github.com/FZambia/go-sentinel"
	"github.com/garyburd/redigo/redis"
	"github.com/nu7hatch/gouuid"
	"log"
	"strings"
	"time"
	"errors"
	// "github.com/vmihailenco/msgpack"  // screwed up types after decoding
	"encoding/json"
)

type broadcast struct {
	host    string
	port    string
	pub     redis.PubSubConn
	pubpool *redis.Pool
	sub     redis.PubSubConn
	prefix  string
	uid     string
	key     string
	remote  bool
	rooms   cmap_string_cmap.ConcurrentMap
}

func newPool(master_addr string, redis_password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   500,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", master_addr)
			if err != nil {
				panic(err)
			}

			if _, err := c.Do("AUTH", redis_password); err != nil {
				c.Close()
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if !sentinel.TestRole(c, "master") {
				return errors.New("Role check failed")
			} else {
				return nil
			}
		},
	}
}

//
// opts: {
//   "host": "127.0.0.1",
//   "port": "6379"
//   "prefix": "socket.io"
// }
func Redis(opts map[string]string, sentinel_infos []string, sentinel_password string) socketio.BroadcastAdaptor {
	b := broadcast{
		rooms: cmap_string_cmap.New(),
	}

	sntnl := &sentinel.Sentinel{
		Addrs: sentinel_infos,
		MasterName: "mymaster",
		Dial: func(addr string) (redis.Conn, error) {
			timeout := 500 * time.Millisecond
			c, err := redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
			if err != nil {
				panic(err)
			}

      if sentinel_password != "" {
        if _, err := c.Do("AUTH", sentinel_password); err != nil {
          c.Close()
          return nil, err
        }
      }

			return c, nil
		},
	}

	master_addr, err := sntnl.MasterAddr()
	if err != nil {
		panic(err)
	}

	master_addr_host := strings.Split(master_addr, ":")[0]
	master_addr_port := strings.Split(master_addr, ":")[1]

	if master_addr_host != "" {
		b.host = master_addr_host
	} else {
		b.host = "127.0.0.1"
	}

	if master_addr_port != "" {
		b.port = master_addr_port
	} else {
		b.port = "6379"
	}

	var ok bool
	b.prefix, ok = opts["prefix"]
	if !ok {
		b.prefix = "socket.io"
	}

	pub, err := redis.Dial("tcp", b.host + ":" + b.port)
	if err != nil {
		panic(err)
	}
  if opts["password"] != "" {
    if _, err := pub.Do("AUTH", opts["password"]); err != nil {
      pub.Close()
      panic(err)
    }
  }


	b.pubpool = newPool(b.host + ":" + b.port, opts["password"])

	sub, err := redis.Dial("tcp", b.host + ":" + b.port)
	if err != nil {
		panic(err)
	}

  if opts["password"] != "" {
    if _, err := sub.Do("AUTH", opts["password"]); err != nil {
      sub.Close()
      panic(err)
    }
  }

	b.pub = redis.PubSubConn{Conn: pub}
	b.sub = redis.PubSubConn{Conn: sub}

	uid, err := uuid.NewV4()
	if err != nil {
		log.Println("error generating uid:", err)
		return nil
	}
	b.uid = uid.String()
	b.key = b.prefix + "#" + b.uid

	b.remote = false

	b.sub.PSubscribe(b.prefix + "#*")

	// This goroutine receives and prints pushed notifications from the server.
	// The goroutine exits when there is an error.
	go func() {
		for {
			switch n := b.sub.Receive().(type) {
			case redis.Message:
				// log.Printf("Message: %s %s\n", n.Channel, n.Data)
			case redis.PMessage:
				b.onmessage(n.Channel, n.Data)
				// log.Printf("PMessage: %s %s %s\n", n.Pattern, n.Channel, n.Data)
			case redis.Subscription:
				// log.Printf("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
				if n.Count == 0 {
					return
				}
			case error:
				log.Printf("error: %v\n", n)
				return
			}
		}
	}()

	return b
}

func (b broadcast) onmessage(channel string, data []byte) error {
	pieces := strings.Split(channel, "#")
	uid := pieces[len(pieces)-1]
	if b.uid == uid {
		// 这里是收到自己发出的消息
		// log.Println("ignore same uid")
		return nil
	}

	var out map[string][]interface{}
	err := json.Unmarshal(data, &out)
	if err != nil {
		log.Println("error decoding data")
		return nil
	}

	args := out["args"]
	opts := out["opts"]
	ignore, ok := opts[0].(socketio.Socket)
	if !ok {
		// 这里是收到别人发出的消息
		// log.Println("ignore is not a socket")
		ignore = nil
	}
	room, ok := opts[1].(string)
	if !ok {
		log.Println("room is not a string")
		room = ""
	}
	message, ok := opts[2].(string)
	if !ok {
		log.Println("message is not a string")
		message = ""
	}

	b.remote = true
	b.Send(ignore, room, message, args...)
	return nil
}

func (b broadcast) Join(room string, socket socketio.Socket) error {
	sockets, ok := b.rooms.Get(room)
	if !ok {
		sockets = cmap_string_socket.New()
	}
	sockets.Set(socket.Id(), socket)
	b.rooms.Set(room, sockets)
	return nil
}

func (b broadcast) Leave(room string, socket socketio.Socket) error {
	sockets, ok := b.rooms.Get(room)
	if !ok {
		return nil
	}
	sockets.Remove(socket.Id())
	if sockets.IsEmpty() {
		b.rooms.Remove(room)
		return nil
	}
	b.rooms.Set(room, sockets)
	return nil
}

// Same as Broadcast
func (b broadcast) Send(ignore socketio.Socket, room, message string, args ...interface{}) error {
	// log.Printf("broadcast Send start %v , %v, %v ", room, message, b.remote)
	sockets, ok := b.rooms.Get(room)
	if ok {
		for item := range sockets.Iter() {
			id := item.Key
			s := item.Val
			if ignore != nil && ignore.Id() == id {
				continue
			}
			err := (s.Emit(message, args...))
			if err != nil {
				log.Println("error broadcasting:", err)
			}
		}
	}

	opts := make([]interface{}, 3)
	opts[0] = ignore
	opts[1] = room
	opts[2] = message
	in := map[string][]interface{}{
		"args": args,
		"opts": opts,
	}

	buf, err := json.Marshal(in)
	_ = err

	if !b.remote {
		// log.Printf("broadcast Send Pushlist %v , %v, %v ", b.remote, b.key, buf)
		conn := b.pubpool.Get()
		defer conn.Close()
		conn.Do("PUBLISH", b.key, buf)
	}
	b.remote = false
	return nil
}
