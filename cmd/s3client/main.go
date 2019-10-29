package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/minio/minio-go"
)

// S3Target contains address and credentials of a S3 endpoint.
type S3Target struct {
	SourceFile    string
	Key           string `json:"key"`
	Endpoint      string `json:"endpoint"`
	Secure        bool   `json:"secure"`
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	DefaultBucket string `json:"defaultBucket"`

	//TODO read-only mode for production safety?
}

var (
	// application and connection state
	currentTarget S3Target
	minioClient   *minio.Client
	currentBucket string
	currentPrefix string
)

func main() {
	// command to execute
	args := make([]string, 0)

	// temporary parser state
	envKey := ""
	envKeyMode := false

	for i := 1; i < len(os.Args); i++ {
		if envKeyMode {
			// read environment key and return to normal command line parser state
			envKey = os.Args[i]
			envKeyMode = false

		} else {
			// only read environment key once -> further "-e" args might be part of actual command
			if len(envKey) == 0 && os.Args[i] == "-e" {
				// next parameter contains the environment key
				envKeyMode = true
			} else {
				// append to command
				args = append(args, os.Args[i])
			}
		}
	}

	if len(envKey) == 0 && len(args) > 0 {
		// the user seems helpless
		printlnf("Usage:")
		printlnf("  - Create new environment with \"-e {name}\" and use with same arguments")
		printlnf("  - Type \"help\" to see a list of available commands")
		os.Exit(1)
	}

	target, err := prepareEnv(envKey)
	if err != nil {
		printlnf(err.Error())
		os.Exit(1)
	}

	if err := connect(target); err != nil {
		printlnf(err.Error())
		os.Exit(1)
	}

	//TODO some connection check?
	if len(target.DefaultBucket) > 0 {
		if err := enter([]string{target.DefaultBucket}); err != nil {
			currentBucket = ""
			printlnf(err.Error())
		}
	}

	if len(args) > 0 {
		// command specified as input? execute and then exit
		if err := execLine(args); err != nil {
			printlnf(err.Error())
			os.Exit(1)
		}

	} else {
		// interactive mode
		if err := browse(); err != nil {
			printlnf(err.Error())
			os.Exit(1)
		}
	}
}

func connect(target S3Target) error {
	currentTarget = target
	client, err := minio.New(target.Endpoint, target.AccessKey, target.SecretKey, target.Secure)
	if err != nil {
		return err
	}
	minioClient = client
	currentBucket = target.DefaultBucket
	currentPrefix = ""
	return nil
}

func browse() error {
	for {
		cmd, err := readCmd()
		if err != nil {
			return err
		}

		// ignore empty commands -> same behavior as bash
		if len(cmd) > 0 {
			command := strings.TrimSpace(cmd[0])
			switch command {
			case "q":
				fallthrough
			case "exit":
				return nil

			//TODO envmod and envdel command?

			case "":
				// do nothing here -> same behavior as bash

			default:
				if err = execCommand(command, cmd[1:]); err != nil {
					printlnf("ERR: %s", err.Error())
				}
			}
		}
	}
}

func init() {
	commands["help"] = help
	commands["enter"] = enter
	commands["leave"] = leave
	commands["cd"] = cd
	commands["ls"] = ls
	commands["rm"] = rm
	commands["dl"] = dl
	commands["ul"] = ul
	commands["mv"] = mv
	commands["cp"] = cp
	commands["touch"] = touch
	commands["cat"] = cat
	commands["find"] = find
	commands["list"] = list
	commands["mkbucket"] = mkbucket
	commands["rmbucket"] = rmbucket
}

var (
	commands = make(map[string]func(args []string) error)
)

func execLine(cmd []string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("No command specified")
	} else if len(cmd) == 1 {
		return execCommand(cmd[0], []string{})
	} else {
		return execCommand(cmd[0], cmd[1:])
	}
}

func execCommand(cmd string, args []string) error {
	f, ok := commands[cmd]
	if ok {
		return f(args)
	}

	return fmt.Errorf("unknown command %q. Use \"help\" to show a list of available commands", cmd)
}
