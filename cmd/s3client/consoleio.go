package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var (
	colorWarning   = "\033[1;31m"
	colorHighlight = "\033[1;31m"
	colorTarget    = "\033[1;32m"
	colorPrefix    = "\033[1;34m"
	colorEnd       = "\033[0m"
)

func init() {
	reader = bufio.NewReader(os.Stdin)

	//TODO disable colors?
}

var (
	reader *bufio.Reader
)

func readCmd() ([]string, error) {
	if len(currentBucket) > 0 {
		if len(currentPrefix) > 0 {
			fmt.Printf(colorTarget+"{%s@%s}"+colorEnd+colorPrefix+"%s"+colorEnd+"> ", currentBucket, currentTarget.Key, currentPrefix)
		} else {
			fmt.Printf(colorTarget+"{%s@%s}"+colorEnd+"> ", currentBucket, currentTarget.Key)
		}
	} else {
		fmt.Printf(colorTarget+"{%s}"+colorEnd+"> ", currentTarget.Key)
	}

	//TODO could be a bit more advanced for convenience
	//TODO maybe re-usable readln with provider functions for auto-complete and history?

	var sb strings.Builder
	escape := false
	doubleQuote := false
	singleQuote := false

	cmd := make([]string, 0)

	for {
		if sb.Len() > 0 {
			// show empty prompt on new lines
			fmt.Print("> ")
		}

		line, err := readln()
		if err != nil {
			return nil, err
		}

		for _, r := range line {
			if singleQuote {
				if r == '\'' {
					singleQuote = false
				} else {
					sb.WriteRune(r)
				}

			} else if doubleQuote {
				if escape {
					sb.WriteRune(r)
					escape = false

				} else {
					if r == '"' {
						doubleQuote = false
					} else if r == '\\' {
						escape = true
					} else {
						sb.WriteRune(r)
					}
				}
			} else if escape {
				sb.WriteRune(r)
				escape = false

			} else {
				if r == '\\' {
					escape = true
				} else if r == '\'' {
					singleQuote = true
				} else if r == '"' {
					doubleQuote = true
				} else if r == ' ' {
					if sb.Len() > 0 {
						cmd = append(cmd, sb.String())
						sb.Reset()
					}
				} else {
					sb.WriteRune(r)
				}
			}
		}

		if !escape && !doubleQuote && !singleQuote {
			break
		}

		// append line break (in quote or escaped)
		sb.WriteRune('\n')
	}

	if sb.Len() > 0 {
		cmd = append(cmd, sb.String())
	}

	return cmd, nil
}

func readln() (string, error) {
	/*buffer := make([]byte, 1024)

	var sb strings.Builder

	for !strings.HasSuffix(sb.String(), "\n") {
		n, err := os.Stdin.Read(buffer)
		if err != nil {
			return "", err
		}

		sb.Write(buffer[:n])
	}

	return sb.String(), nil*/

	// does not offer any helper
	text, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return text[:len(text)-1], nil
}

func readlnNonEmpty() (string, error) {
	line, err := readln()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", errUserAbort{}
	}
	return line, nil
}

func println(format string, args ...interface{}) {
	fmt.Println(fmt.Sprintf(format, args...))
}

type errUserAbort struct{}

func (errUserAbort) Error() string { return "aborted by user" }
