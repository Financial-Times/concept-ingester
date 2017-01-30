package main

import (
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"log"
	"regexp"
	"strings"
)

// FT async msg format:
//
// message-version CRLF
// *(message-header CRLF)
// CRLF
// message-body
func parseMessage(raw []byte) (m queueConsumer.Message, err error) {
	decoded := string(raw[:])
	doubleNewLineStartIndex := getHeaderSectionEndingIndex(string(decoded[:]))
	if m.Headers, err = parseHeaders(string(decoded[:doubleNewLineStartIndex])); err != nil {
		return
	}
	m.Body = strings.TrimSpace(string(decoded[doubleNewLineStartIndex:]))
	return
}

func getHeaderSectionEndingIndex(msg string) int {
	//FT msg format uses CRLF for line endings
	i := strings.Index(msg, "\r\n\r\n")
	if i != -1 {
		return i
	}
	//fallback to UNIX line endings
	i = strings.Index(msg, "\n\n")
	if i != -1 {
		return i
	}
	log.Printf("WARN  - message with no message body: [%s]", msg)
	return len(msg)
}

var re = regexp.MustCompile("[\\w-]*:[\\w\\-:/. ]*")

var kre = regexp.MustCompile("[\\w-]*:")
var vre = regexp.MustCompile(":[\\w-:/. ]*")

func parseHeaders(msg string) (map[string]string, error) {
	headerLines := re.FindAllString(msg, -1)

	headers := make(map[string]string)
	for _, line := range headerLines {
		key, value := parseHeader(line)
		headers[key] = value
	}
	return headers, nil
}

func parseHeader(header string) (string, string) {
	key := kre.FindString(header)
	value := vre.FindString(header)
	return key[:len(key)-1], strings.TrimSpace(value[1:])
}
