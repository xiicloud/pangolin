package pangolin

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Command uint32

const (
	CmdJoin      = Command(1)
	CmdWorker    = Command(2)
	MinRequestId = 100
)

var Endian = binary.LittleEndian

type Protocol struct {
	auth Authenticator
}

// JoinId reads the agent ID from the network connection.
// This function should be called on the server side.
func (p Protocol) GetAgentId(conn net.Conn) (id string, err error) {
	var (
		idLen  uint32
		keyLen uint32
	)
	if err = binary.Read(conn, Endian, &idLen); err != nil {
		return
	}
	if idLen < 1 {
		return "", fmt.Errorf("unable to recoginze the protocol")
	}

	idBytes := make([]byte, idLen)
	if err = binary.Read(conn, Endian, &idBytes); err != nil {
		return
	}

	if p.auth == nil {
		return string(idBytes), nil
	}

	// Read the auth credentials.
	if err = binary.Read(conn, Endian, &keyLen); err != nil {
		return
	}
	key := make([]byte, keyLen)
	if keyLen > 0 {
		err = binary.Read(conn, Endian, &key)
		if err != nil {
			return "", err
		}
	}
	if err := p.auth.Auth(string(idBytes), string(key)); err != nil {
		return "", err
	}

	return string(idBytes), nil
}

// Join sends the agent ID to the server.
func (p Protocol) Join(conn net.Conn, id []byte) error {
	data := []interface{}{
		CmdJoin,
		uint32(len(id)),
		id,
	}

	if p.auth != nil {
		key := []byte(p.auth.Token())
		data = append(data, uint32(len(key)), key)
	}

	for _, v := range data {
		if err := binary.Write(conn, Endian, v); err != nil {
			return err
		}
	}
	return nil
}

// NewWorker sends the new_worker command to the agent
// or report the request id to the server.
func (p Protocol) NewWorker(conn net.Conn, id uint32) error {
	return binary.Write(conn, Endian, id)
}

// GetCmd recognizes the command from the first 4 bytes of the connection.
func (p Protocol) GetCmd(conn net.Conn) (Command, error) {
	var cmd Command
	err := binary.Read(conn, Endian, &cmd)
	return cmd, err
}
