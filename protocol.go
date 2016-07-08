package pangolin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Sirupsen/logrus"
)

type Command uint32

const (
	CmdJoin      = Command(1)
	CmdWorker    = Command(2)
	CmdData      = Command(3)
	MinRequestId = 100
	Version      = "1"
)

var byteOrder = binary.LittleEndian

type Protocol struct {
	auth Authenticator
}

// JoinId reads the agent ID from the network connection.
// This function should be called on the server side.
func (p Protocol) getAgentId(conn io.Reader) (id string, err error) {
	var (
		idLen  uint32
		keyLen uint32
	)
	if err = binary.Read(conn, byteOrder, &idLen); err != nil {
		return
	}
	if idLen < 1 {
		return "", fmt.Errorf("unable to recoginze the protocol")
	}

	idBytes := make([]byte, idLen)
	if err = binary.Read(conn, byteOrder, &idBytes); err != nil {
		return
	}

	if p.auth == nil {
		return string(idBytes), nil
	}

	// Read the auth credentials.
	if err = binary.Read(conn, byteOrder, &keyLen); err != nil {
		return
	}
	key := make([]byte, keyLen)
	if keyLen > 0 {
		err = binary.Read(conn, byteOrder, &key)
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
func (p Protocol) join(conn io.Writer, id []byte) error {
	if err := p.sendVersion(conn); err != nil {
		return err
	}

	data := []interface{}{
		CmdJoin,
		uint32(len(id)),
		id,
	}

	if p.auth != nil {
		key := []byte(p.auth.Token())
		data = append(data, uint32(len(key)), key)
	}

	buf := new(bytes.Buffer)
	for _, v := range data {
		if err := binary.Write(buf, byteOrder, v); err != nil {
			return err
		}
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

// newWorker sends the new_worker command to the agent
// or report the request id to the server.
func (p Protocol) newWorker(conn io.Writer, id uint32, sendHeader bool) error {
	if sendHeader {
		err := p.sendVersion(conn)
		if err != nil {
			return err
		}
	}

	return binary.Write(conn, byteOrder, id)
}

// GetCmd recognizes the command from the first 4 bytes of the connection.
func (p Protocol) getAgentCmd(conn io.Reader) (Command, error) {
	return p.getCmd(conn, false)
}

// GetCmd recognizes the command from the first 4 bytes of the connection.
func (p Protocol) getServerCmd(conn io.Reader) (Command, error) {
	return p.getCmd(conn, true)
}

func (p Protocol) getCmd(conn io.Reader, stripHeader bool) (Command, error) {
	if !stripHeader {
		_ = p.getVersion(conn)
	}
	var cmd Command
	err := binary.Read(conn, byteOrder, &cmd)
	return cmd, err
}

func (p Protocol) sendVersion(conn io.Writer) error {
	_, err := conn.Write([]byte("PGL" + Version))
	return err
}

func (p Protocol) getVersion(conn io.Reader) string {
	// discard the protocol marker
	header := make([]byte, 4)
	_, err := io.ReadAtLeast(conn, header, 4)
	if err != nil {
		logrus.Errorf("pangolin.getVersion: ", err)
		return ""
	}
	return string(header[3:])
}
