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
	target, args, err := readArgs()
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

			if len(target.SourceFile) == 0 && len(args) > 0 {
				// the target was specified by command line and a command is given -> fail now for automation
				os.Exit(1)
			}
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

func readArgs() (S3Target, []string, error) {
	// command to execute
	args := make([]string, 0)

	// temporary parser state
	envKey := ""
	argParseMode := ""

	var targetName, targetURL, targetAccessKey, targetSecretKey, targetBucketName string

	for i := 1; i < len(os.Args); i++ {
		nextArgParseMode := ""

		switch argParseMode {
		case "-e":
			// read environment key and return to normal command line parser state
			envKey = os.Args[i]

		case "--name":
			targetName = os.Args[i]
		case "--url":
			targetURL = os.Args[i]
		case "--access-key":
			targetAccessKey = os.Args[i]
		case "--secret-key":
			targetSecretKey = os.Args[i]
		case "--bucket-name":
			targetBucketName = os.Args[i]

		case "":
			// only read environment key once -> further "-e" args might be part of actual command
			if len(envKey) == 0 && os.Args[i] == "-e" {
				// next parameter contains the environment key
				nextArgParseMode = "-e"
			} else if strings.HasPrefix(os.Args[i], "--") {
				nextArgParseMode = os.Args[i]
			} else {
				// append to command
				args = append(args, os.Args[i])
			}

		default:
			panic(fmt.Sprintf("invalid arg parser state %q", argParseMode))
		}

		argParseMode = nextArgParseMode
	}

	if len(targetName) > 0 || len(targetURL) > 0 || len(targetAccessKey) > 0 || len(targetSecretKey) > 0 || len(targetBucketName) > 0 {
		if len(targetURL) == 0 {
			printlnf("Missing --url parameter")
			os.Exit(1)
		}
		if len(targetAccessKey) == 0 {
			printlnf("Missing --access-key parameter")
			os.Exit(1)
		}
		if len(targetSecretKey) == 0 {
			printlnf("Missing --secret-key parameter")
			os.Exit(1)
		}

		endpoint := targetURL
		secure := true
		if strings.HasPrefix(strings.ToLower(endpoint), "http://") {
			endpoint = endpoint[7:]
			secure = false
		} else if strings.HasPrefix(strings.ToLower(endpoint), "https://") {
			endpoint = endpoint[8:]
			secure = true
		}
		return S3Target{Key: targetName, Endpoint: endpoint, Secure: secure, AccessKey: targetAccessKey, SecretKey: targetSecretKey, DefaultBucket: targetBucketName}, args, nil
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
		return S3Target{}, nil, err
	}
	return target, args, nil
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
