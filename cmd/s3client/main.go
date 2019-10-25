package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
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
		println("Usage:")
		println("  - Create new environment with \"-e {name}\" and use with same arguments")
		println("  - Type \"help\" to see a list of available commands")
		os.Exit(1)
	}

	target, err := prepareEnv(envKey)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	if err := connect(target); err != nil {
		println(err.Error())
		os.Exit(1)
	}

	//TODO some connection check?
	if len(target.DefaultBucket) > 0 {
		if err := enter([]string{target.DefaultBucket}); err != nil {
			currentBucket = ""
			println(err.Error())
		}
	}

	if len(args) > 0 {
		// command specified as input? execute and then exit
		if err := execLine(args); err != nil {
			println(err.Error())
			os.Exit(1)
		}

	} else {
		// interactive mode
		if err := browse(); err != nil {
			println(err.Error())
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
					println("ERR: %s", err.Error())
				}
			}
		}
	}
}

func init() {
	commands["help"] = printHelp
	commands["enter"] = enter
	commands["leave"] = leave
	commands["cd"] = cd
	commands["ls"] = ls
	commands["rm"] = rm
	commands["dl"] = dl
	commands["ul"] = ul
	//TODO cat
	//TODO find
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

/* ################################################ */
/* ###                 commands                 ### */
/* ################################################ */

func printHelp(args []string) error {
	println("Available commands:")
	println("  exit             -  exit application")
	println("  help             -  show this help")
	println("  enter {name}     -  enter bucket with given name")
	println("  leave            -  leave current bucket")
	println("  cd               -  enter named directory or \"..\" for parent dir")
	println("  ls               -  list objects in current bucket and path")
	println("  rm {name}        -  remove object. Use \"-r\" flag to remove all prefixed objects")
	println("  dl {src} {dst}   -  download a remote file {src} and write to local file {dst}")
	println("  ul {src} {dst}   -  upload local file {src} to remote file {dst}")
	println("  list {type}      -  list items of any type in [bucket, object, env]")
	println("  mkbucket {name}  -  create new bucket with given name")
	println("  rmbucket {name}  -  delete bucket with given name")
	return nil
}

func enter(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no bucket name specified")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	exists, err := minioClient.BucketExists(args[0])
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("bucket %q does not exist", args[0])
	}

	currentBucket = args[0]
	currentPrefix = ""
	return nil
}

func leave(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("too many arguments")
	}

	currentBucket = ""
	currentPrefix = ""
	return nil
}

func cd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no directory specified")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	if len(currentBucket) == 0 {
		println("No bucket entered yet. Entering bucket %q instead", args[0])
		return enter([]string{args[0]})
	}

	//TODO check existence?

	if args[0] == ".." {
		prefix := strings.TrimRight(currentPrefix, "/")
		parts := strings.Split(prefix, "/")
		if len(parts) > 1 {
			currentPrefix = strings.Join(parts[:len(parts)-1], "/") + "/"
		} else {
			currentPrefix = ""
		}
	} else {
		currentPrefix += args[0] + "/"
	}

	return nil
}

func ls(args []string) error {
	if len(currentBucket) == 0 {
		println("No bucket entered yet. Listing buckets instead")
		return list([]string{"bucket"})
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	prefix := currentPrefix
	if len(args) > 0 {
		if strings.HasSuffix(prefix, "/") {
			prefix = args[0]
		} else {
			prefix = args[0] + "/"
		}
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	hasFiles := false

	list := make([]minio.ObjectInfo, 0)
	objectCh := minioClient.ListObjectsV2(currentBucket, prefix, false, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			return fmt.Errorf("failed to access object: %v", obj.Err)
		}

		list = append(list, obj)
		if !strings.HasSuffix(obj.Key, "/") {
			hasFiles = true
		}
	}

	if len(list) == 0 {
		println("No objects found.")
	} else {
		if len(list) == 1 {
			println("Found 1 object:")
		} else {
			println("Found %d objects:", len(list))
		}

		dirPadding := ""
		if hasFiles {
			// humanized file size: "1000.00 GiB" -> 11
			// padding to file name -> 2
			// => 13
			dirPadding = strings.Repeat(" ", 13)
		}

		for _, obj := range list {
			if strings.HasSuffix(obj.Key, "/") {
				println("  D  %s%s", dirPadding, obj.Key[len(prefix):len(obj.Key)-1])
			} else {
				sizeStr := humanize.IBytes(uint64(obj.Size))
				if strings.HasSuffix(sizeStr, " B") {
					// align actual numbers of 1-letter unit 'Byte' with 3-letter units like 'MiB'
					sizeStr = sizeStr + "  "
				}
				padding := strings.Repeat(" ", 11-len(sizeStr))
				println("  F  %s%s  %s", padding, sizeStr, obj.Key[len(prefix):])
			}
		}
	}

	return nil
}

func rm(args []string) error {
	return fmt.Errorf("The command \"rm\" is not yet implemented")
}

func dl(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no source object specified")
	}
	if len(args) == 1 {
		return fmt.Errorf("no local destination file specified")
	}

	//TODO check object exists

	objKey := currentPrefix + args[0]
	println("Source Object: %s", objKey)

	obj, err := minioClient.GetObject(currentBucket, objKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()

	f, err := os.Create(args[1])
	if err != nil {
		return err
	}
	defer f.Close()

	//TODO download with status bar
	len, err := io.Copy(f, obj)
	if err != nil {
		return err
	}

	println("Completed: %s", humanize.IBytes(uint64(len)))
	return nil
}

func ul(args []string) error {
	return fmt.Errorf("The command \"ul\" is not yet implemented")
}

func list(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no item type specified. Type \"list bucket\" to show a list of all buckets")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	switch args[0] {
	case "buckets":
		fallthrough
	case "bucket":
		buckets, err := minioClient.ListBuckets()
		if err != nil {
			return err
		}

		if len(buckets) == 0 {
			println("No buckets found. Use \"mkbucket {name}\" to create one")
		} else {
			if len(buckets) == 1 {
				println("Found 1 bucket:")
			} else {
				println("Found %d buckets:", len(buckets))
			}
			for _, b := range buckets {
				println("  B  %s", b.Name)
			}
		}

	default:
		return fmt.Errorf("unkown list type %q. Possible parameters are \"bucket\", \"object\" and \"env\"", args[0])
	}
	return nil
}

func mkbucket(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no bucket name given")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	bucketName := args[0]
	err := minioClient.MakeBucket(bucketName, "")
	if err != nil {
		return err
	}

	if len(currentBucket) == 0 {
		currentBucket = bucketName
	}
	println("bucket %q created", bucketName)
	return nil
}

func rmbucket(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no bucket name given")
	}
	//TODO --i-know-what-i-do flag to skip questions
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	bucketName := args[0]
	exists, err := minioClient.BucketExists(bucketName)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("bucket %q does not exist", bucketName)
	}

	println(colorWarning + "########################################")
	println("###  WARNING: POSSIBLE LOSS OF DATA  ###")
	println("########################################" + colorEnd)
	println("You are about to delete bucket %q.", bucketName)
	println("All data stored in this bucket will be lost and cannot be restored!")
	println("Please confirm deletion by entering the bucket name below:")
	fmt.Print("> ")
	str, err := readln()
	if err != nil {
		return err
	}

	if str != bucketName {
		println("Input mismatch. Bucket was NOT deleted")
		return nil
	}

	println(colorWarning + "#########################################")
	println("###  WARNING: THIS CAN NOT BE UNDONE  ###")
	println("#########################################" + colorEnd)
	println("Are you sure? Please enter DELETE to finally delete the bucket:")
	fmt.Print("> ")
	strDELETE, err := readln()
	if err != nil {
		return err
	}

	if strDELETE != "DELETE" {
		println("Abort. Bucket was NOT deleted")
		return nil
	}

	if err := minioClient.RemoveBucket(bucketName); err != nil {
		return err
	}

	println("Bucket %q has been deleted", bucketName)
	if currentBucket == bucketName {
		// leave deleted bucket if it was entered
		currentBucket = ""
		currentPrefix = ""
	}
	return nil
}

/* ################################################ */
/* ###            console io helper             ### */
/* ################################################ */

var (
	colorWarning = "\033[1;31m"
	colorTarget  = "\033[1;32m"
	colorPrefix  = "\033[1;34m"
	colorEnd     = "\033[0m"
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
