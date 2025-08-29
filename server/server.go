package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/redis"
)

// RedisServer represents a simple Redis server implementation
type RedisServer struct {
	address string
	redis   *redis.RedisWrapper
	ln      net.Listener
	wg      sync.WaitGroup
	quit    chan interface{}
}

// NewRedisServer creates a new Redis server instance
func NewRedisServer(address string, dbPath string) (*RedisServer, error) {
	// Initialize config
	cfg := config.DefaultConfig()
	config.SetGlobalConfig(cfg)

	// Create Redis wrapper
	redisWrapper, err := redis.NewRedisWrapper(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis wrapper: %w", err)
	}

	return &RedisServer{
		address: address,
		redis:   redisWrapper,
		quit:    make(chan interface{}),
	}, nil
}

// Start starts the Redis server
func (s *RedisServer) Start() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.ln = ln

	log.Printf("Redis server listening on %s", s.address)

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	return nil
}

// acceptConnections handles incoming client connections
func (s *RedisServer) acceptConnections() {
	defer s.wg.Done()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.quit:
				return
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle each connection in a separate goroutine
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes commands from a client connection
func (s *RedisServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		// Read RESP command
		args, err := s.readRESPCommand(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading command: %v", err)
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		// Process command
		response := s.processCommand(args)

		// Send response
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}

// readRESPCommand reads a RESP command from the reader
func (s *RedisServer) readRESPCommand(reader *bufio.Reader) ([]string, error) {
	// Read array header
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array header: %s", line)
	}

	// Parse number of arguments
	var argCount int
	_, err = fmt.Sscanf(strings.TrimSpace(line), "*%d", &argCount)
	if err != nil {
		return nil, fmt.Errorf("invalid argument count: %w", err)
	}

	if argCount <= 0 {
		return []string{}, nil
	}

	// Read each argument
	args := make([]string, argCount)
	for i := 0; i < argCount; i++ {
		// Read bulk string header
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		if len(line) < 2 || line[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk string header: %s", line)
		}

		// Parse string length
		var strLen int
		_, err = fmt.Sscanf(strings.TrimSpace(line), "$%d", &strLen)
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %w", err)
		}

		// Read the actual string
		strBytes := make([]byte, strLen+2) // +2 for \r\n
		_, err = io.ReadFull(reader, strBytes)
		if err != nil {
			return nil, err
		}

		// Remove \r\n and store argument
		args[i] = string(strBytes[:strLen])
	}

	return args, nil
}

// processCommand processes a Redis command and returns the response
func (s *RedisServer) processCommand(args []string) string {
	if len(args) == 0 {
		return "-ERR empty command\r\n"
	}

	command := strings.ToUpper(args[0])

	switch command {
	case "PING":
		if len(args) > 1 {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		}
		return "+PONG\r\n"
	case "SET":
		return s.redis.Set(args)
	case "GET":
		return s.redis.Get(args)
	case "DEL":
		return s.redis.Del(args)
	case "INCR":
		return s.redis.Incr(args)
	case "DECR":
		return s.redis.Decr(args)
	case "HSET":
		return s.redis.HSet(args)
	case "HGET":
		return s.redis.HGet(args)
	// case "HGETALL":
	// return s.redis.HGetAll(args)
	case "EXPIRE":
		return s.redis.Expire(args)
	case "TTL":
		return s.redis.TTL(args)
	case "LPUSH":
		return s.redis.LPush(args)
	case "RPUSH":
		return s.redis.RPush(args)
	case "LPOP":
		return s.redis.LPop(args)
	case "RPOP":
		return s.redis.RPop(args)
	case "LLEN":
		return s.redis.LLen(args)
	case "SADD":
		return s.redis.SAdd(args)
	case "SREM":
		return s.redis.SRem(args)
	case "SISMEMBER":
		return s.redis.SIsMember(args)
	case "SMEMBERS":
		return s.redis.SMembers(args)
	case "ZADD":
		return s.redis.ZAdd(args)
	case "ZREM":
		return s.redis.ZRem(args)
	case "ZRANGE":
		return s.redis.ZRange(args)
	case "ZCARD":
		return s.redis.ZCard(args)
	case "FLUSHALL":
		err := s.redis.FlushAll()
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return "+OK\r\n"
	case "QUIT":
		return "+OK\r\n"
	default:
		return "-ERR unknown command '" + command + "'\r\n"
	}
}

// Stop stops the Redis server gracefully
func (s *RedisServer) Stop() error {
	if s.ln != nil {
		// Close the listener to stop accepting new connections
		s.ln.Close()
	}

	// Signal to stop accepting connections
	close(s.quit)

	// Wait for all goroutines to finish
	s.wg.Wait()

	// Close the Redis wrapper
	if s.redis != nil {
		s.redis.Close()
	}

	return nil
}
