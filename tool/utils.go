package tool

import (
	"errors"
	"strconv"
	"strings"
)

type RuntimeError struct {
	Stage string
	Err   error
}

func (re *RuntimeError) Error() string {
	return re.Stage + ": " + re.Err.Error()
}

func Check(addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}
	m := make(map[string]byte)
	for _, address := range addresses {
		if len(address) == 0 {
			return errors.New("empty ip address")
		}
		sp := strings.Split(address, ":")
		if len(sp) == 1 {
			return errors.New(address + "'s port is not specified")
		}
		port, err := strconv.Atoi(sp[1])
		if err != nil || port <= 0 {
			return errors.New("port " + sp[1] + " is invalid")
		}
		ipAddr := sp[0]
		if "localhost" != ipAddr && !valid(ipAddr) {
			return errors.New(ipAddr + " is not a valid ip address")
		}
		if _, duplicate := m[address]; duplicate {
			return errors.New(address + " is duplicate")
		}
		m[address] = 1
	}
	return nil
}

func valid(queryIP string) bool {
	if sp := strings.Split(queryIP, "."); len(sp) == 4 {
		for _, s := range sp {
			if len(s) > 1 && s[0] == '0' {
				return false
			}
			if v, err := strconv.Atoi(s); err != nil || v > 255 {
				return false
			}
		}
		return true
	}
	if sp := strings.Split(queryIP, ":"); len(sp) == 8 {
		for _, s := range sp {
			if len(s) > 4 {
				return false
			}
			if _, err := strconv.ParseUint(s, 16, 64); err != nil {
				return false
			}
		}
		return true
	}
	return false
}
