package memcached

import (
	"errors"
	"time"
)

var (
	ErrCacheMiss = errors.New("memcache: cache miss")
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")
	ErrNotStored = errors.New("memcache: item not stored")
	ErrExists = errors.New("memcached: item already exists (cas)")
	ErrNotFound = errors.New("memcached: item not found (cas)")
	ErrServerError = errors.New("memcache: server error")
	ErrNoStats = errors.New("memcache: no stats available")
	ErrKeyTooLong = errors.New("memcache: key is too long")
	ErrMalformedKey = errors.New("memcache: key contains invalid characters")
	ErrNoServers = errors.New("memcache: no servers available")
	ErrClientError = errors.New("memcache: inexistent command")
)

var (
	ErrTooManyConns = errors.New("client: too many connections")
	ErrNoConnections = errors.New("client: no connections available")
)

var (
	END 				= 	[]byte("END\r\n")
	CRLF               	= 	[]byte("\r\n")
	RES_NONEX_COMMAND  	= 	[]byte("ERROR\r\n")
	RES_CLIENT_ERROR 	= 	[]byte("CLIENT ERROR ")
	RES_SERVER_ERROR 	= 	[]byte("SERVER ERROR ")
	MEM_STORED 			= 	[]byte("STORED\r\n")
	MEM_NOT_STORED 		= 	[]byte("NOT_STORED\r\n")
	MEM_EXISTS 			= 	[]byte("EXISTS\r\n")
	MEM_NOT_FOUND 		= 	[]byte("NOT_FOUND\r\n")
	MEM_ERROR 			=   []byte("ERROR\r\n")
	MEM_DELETED 	    =   []byte("DELETED\r\n")
	MEM_TOUCHED			= 	[]byte("TOUCHED\r\n")
)

const (
	DefaultTimeout = 100 * time.Millisecond
	DefaultMaxIdleConns = 2
)