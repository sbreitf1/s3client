package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/minio/minio-go"

	"github.com/alecthomas/kingpin"
)

// S3Target contains address and credentials of a S3 endpoint.
type S3Target struct {
	Key           string `json:"key"`
	Endpoint      string `json:"endpoint"`
	Secure        bool   `json:"secure"`
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	DefaultBucket string `json:"defaultBucket"`

	//TODO read-only mode for production safety?
}

var (
	appMain = kingpin.New("s3client", "Browse and manage any S3 endpoint")
	envFile = appMain.Flag("env", "Name of environment to use or create").Required().Short('e').String()

	// application and connection state
	targetKey     string
	minioClient   *minio.Client
	currentBucket string
	currentPrefix string
)

func main() {
	kingpin.MustParse(appMain.Parse(os.Args[1:]))

	var target S3Target
	if len(*envFile) == 0 {
		//TODO create temporary target?
		log.Fatalln("No environment specified.")

	} else {
		t, err := prepareEnv(*envFile)
		if err != nil {
			log.Fatalln(err.Error())
		}
		target = t
	}

	if err := connect(target); err != nil {
		log.Fatalln(err.Error())
	}

	//TODO some connection check?
	if len(target.DefaultBucket) > 0 {
		if err := enter([]string{target.DefaultBucket}); err != nil {
			currentBucket = ""
			println(err.Error())
		}
	}

	if err := browse(); err != nil {
		log.Fatalln(err.Error())
	}
}

func prepareEnv(key string) (S3Target, error) {
	usr, err := user.Current()
	if err != nil {
		return S3Target{}, err
	}

	p := path.Join(usr.HomeDir, ".s3client/"+key+".json")
	f, err := os.Open(p)
	if err != nil {
		//TODO allow user to enter environment if not exists
		return S3Target{}, err
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return S3Target{}, fmt.Errorf("unable to read environment file: %s", err.Error())
	}

	var target S3Target
	if err := json.Unmarshal(data, &target); err != nil {
		return S3Target{}, fmt.Errorf("malformed environment file: %s", err.Error())
	}
	return target, nil
}

func connect(target S3Target) error {
	targetKey = target.Key
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
		line, err := readCmd()
		if err != nil {
			return err
		}

		cmd := strings.Split(line, " ")
		switch cmd[0] {
		case "q":
			fallthrough
		case "exit":
			return nil

		//TODO envmod and envdel command?

		case "":
			// do nothing here -> same behavior as bash

		default:
			if err = execCommand(cmd[0], cmd[1:]); err != nil {
				println("ERR: %s", err.Error())
			}
		}
	}
}

func init() {
	commands["help"] = printHelp
	commands["enter"] = enter
	commands["leave"] = leave
	commands["cd"] = cd
	commands["ls"] = printLs
	commands["list"] = printList
	commands["mkbucket"] = mkbucket
}

var (
	commands = make(map[string]func(args []string) error)
)

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
	println("  list {type}      -  list items of any type in [bucket, object, env]")
	println("  mkbucket {name}  -  create new bucket with given name")
	println("  rmbucket {name}  -  delete bucket with given name")
	println("  pwd              -  print current location")
	return nil
}

func enter(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no bucket name specified")
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
	currentBucket = ""
	currentPrefix = ""
	return nil
}

func cd(args []string) error {
	if len(currentBucket) == 0 {
		return fmt.Errorf("no directory specified")
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

func printLs(args []string) error {
	if len(currentBucket) == 0 {
		println("No bucket entered yet. Listing buckets instead")
		return printList([]string{"bucket"})
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

	list := make([]minio.ObjectInfo, 0)
	objectCh := minioClient.ListObjectsV2(currentBucket, prefix, false, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			return fmt.Errorf("failed to access object: %v", obj.Err)
		}

		list = append(list, obj)
	}

	if len(list) == 0 {
		println("No objects found.")
	} else {
		if len(list) == 1 {
			println("Found 1 object:")
		} else {
			println("Found %d objects:", len(list))
		}
		for _, obj := range list {
			if strings.HasSuffix(obj.Key, "/") {
				println("  D  %s", obj.Key[len(prefix):len(obj.Key)-1])
			} else {
				println("  F  %s", obj.Key[len(prefix):])
			}
		}
	}

	return nil
}

func printList(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no item type specified. Type \"list bucket\" to show a list of all buckets")
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
			println("No buckets found. Use \"mkbuckets bucket-name\" to create one")
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
		return fmt.Errorf("unkown list type %q. Possible parameters are \"bucket\" and \"object\"", args[0])
	}
	return nil
}

func mkbucket(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no bucket name given")
	}

	bucketName := strings.Join(args, " ")
	err := minioClient.MakeBucket(bucketName, "")
	if err != nil {
		return err
	}

	println("bucket %q created", bucketName)
	return nil
}

/* ################################################ */
/* ###            console io helper             ### */
/* ################################################ */

var (
	colorTarget = "\033[1;32m"
	colorPrefix = "\033[1;34m"
	colorEnd    = "\033[0m"
)

func init() {
	reader = bufio.NewReader(os.Stdin)

	//TODO disable colors?
}

var (
	reader *bufio.Reader
)

func readCmd() (string, error) {
	if len(currentBucket) > 0 {
		if len(currentPrefix) > 0 {
			fmt.Printf(colorTarget+"{%s@%s}"+colorEnd+colorPrefix+"%s"+colorEnd+"> ", currentBucket, targetKey, currentPrefix)
		} else {
			fmt.Printf(colorTarget+"{%s@%s}"+colorEnd+"> ", currentBucket, targetKey)
		}
	} else {
		fmt.Printf(colorTarget+"{%s}"+colorEnd+"> ", targetKey)
	}
	//TODO could be a bit more advanced for convenience
	//TODO maybe re-usable readln with provider functions for auto-complete and history?
	line, err := readln()
	if err != nil {
		return "", err
	}
	//TODO interpret line quotes
	return line, nil
}

func readln() (string, error) {
	// does not offer any helper
	text, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return text[:len(text)-1], nil
}

func println(format string, args ...interface{}) {
	fmt.Println(fmt.Sprintf(format, args...))
}

type errUserAbort struct{}

func (errUserAbort) Error() string { return "aborted by user" }
