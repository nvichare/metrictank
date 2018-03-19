package schema

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

var ErrStringTooShort = errors.New("string to short")

// Key identifies a metric
type Key [16]byte

// MKey uniquely identifies a metric in a multi-tenant context
type MKey struct {
	Key [16]byte
	Org uint32
}

// KeyFromString parses a string id to an MKey
// string id must be of form orgid.<hexadecimal hash>
func MKeyFromString(s string) (MKey, error) {
	l := len(s)

	// shortest an orgid can be is single digit
	if l < 34 {
		return MKey{}, ErrStringTooShort
	}

	hashStr := s[l-32:]
	orgStr := s[0 : l-33]

	hash, err := hex.DecodeString(hashStr)
	if err != nil {
		return MKey{}, err
	}

	org, err := strconv.ParseUint(orgStr, 10, 32)
	if err != nil {
		return MKey{}, err
	}

	k := MKey{
		Org: uint32(org),
	}

	copy(k.Key[:], hash)
	return k, nil
}

func (m MKey) String() string {
	return fmt.Sprintf("%d.%x", m.Org, m.Key)

}

// AMKey is a multi-tenant key with archive extension
// so you can refer to rollup archives
type AMKey struct {
	MKey    MKey
	Archive Archive
}

func (a AMKey) String() string {
	if a.Archive == 0 {
		return a.MKey.String()
	}
	return a.MKey.String() + "_" + a.Archive.String()
}
