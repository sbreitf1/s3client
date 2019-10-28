package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go"
)

func help(args []string) error {
	println("Available commands:")
	println("  exit             -  exit application")
	println("  help             -  show this help")
	println("  enter {name}     -  enter bucket with given name")
	println("  leave            -  leave current bucket")
	println("  cd               -  enter named directory or \"..\" for parent dir")
	println("  ls               -  list objects in current bucket and path")
	println("  rm {name}        -  remove object. Use \"-r\" flag to remove all prefixed objects recursively")
	println("  dl {src} {dst}   -  download a remote object {src} and write to local file {dst}")
	println("  ul {src} {dst}   -  upload local file {src} to remote object {dst}")
	println("  cat {name}       -  print content of object {name}")
	println("  find {needle}    -  list all objects with given {needle} in last part of object key")
	println("  list {type}      -  list items of any type in [bucket, object, env]")
	println("  mkbucket {name}  -  create new bucket with given name")
	println("  rmbucket {name}  -  delete bucket with given name")
	return nil
}

func enter(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"bucket name"}, MinArgs: 1, RequireBucket: false}); err != nil {
		return err
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
	if err := checkArgs(args, argOptions{ArgLabels: []string{}, MinArgs: 0, RequireBucket: true}); err != nil {
		return err
	}

	currentBucket = ""
	currentPrefix = ""
	return nil
}

func cd(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"dir name"}, MinArgs: 1, RequireBucket: false}); err != nil {
		return err
	}

	if len(args) == 0 {
		//TODO select from list
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
	if err := checkArgs(args, argOptions{ArgLabels: []string{"dir name"}, MinArgs: 0, RequireBucket: false}); err != nil {
		return err
	}

	if len(currentBucket) == 0 {
		println("No bucket entered yet. Listing buckets instead")
		return list([]string{"bucket"})
	}

	prefix := currentPrefix
	if len(args) > 0 {
		if strings.HasSuffix(prefix, "/") {
			prefix = args[0]
		} else {
			prefix = args[0] + "/"
		}
	}

	return printObjects(prefix, nil, nil)
}

func rm(args []string) error {
	return fmt.Errorf("The command \"rm\" is not yet implemented")
}

func dl(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"source", "destination"}, MinArgs: 2, RequireBucket: true}); err != nil {
		return err
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

func cat(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"object name"}, MinArgs: 1, RequireBucket: true}); err != nil {
		return err
	}

	//TODO check object exists and ask for large/binary files

	objKey := currentPrefix + args[0]
	obj, err := minioClient.GetObject(currentBucket, objKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()

	//TODO download with status bar
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, obj); err != nil {
		return err
	}

	println(buffer.String())
	return nil
}

func find(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"needle", "prefix"}, MinArgs: 1, RequireBucket: true}); err != nil {
		return err
	}

	needle := args[0]

	prefix := currentPrefix
	if len(args) > 1 {
		if strings.HasSuffix(prefix, "/") {
			prefix = args[1]
		} else {
			prefix = args[1] + "/"
		}
	}

	return printObjects(prefix,
		func(obj minio.ObjectInfo) bool {
			parts := strings.Split(obj.Key, "/")
			objectName := parts[len(parts)-1]
			if len(objectName) == 0 {
				objectName = parts[len(parts)-2]
			}

			return strings.Contains(strings.ToLower(objectName), strings.ToLower(needle))
		},
		func(name string) string {
			var sb strings.Builder
			for i := 0; i < len(name); {
				relPos := strings.Index(strings.ToLower(name[i:]), strings.ToLower(needle))
				if relPos == -1 {
					sb.WriteString(name[i:])
					break
				}

				if relPos > 0 {
					sb.WriteString(name[i : i+relPos])
				}

				sb.WriteString(colorHighlight)
				sb.WriteString(name[i+relPos : i+relPos+len(needle)])
				sb.WriteString(colorEnd)

				i += relPos + len(needle)
			}
			return sb.String()
		})
}

func list(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"list type"}, MinArgs: 1, RequireBucket: false}); err != nil {
		return err
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

	//TODO object and env

	default:
		return fmt.Errorf("unkown list type %q. Possible parameters are \"bucket\", \"object\" and \"env\"", args[0])
	}
	return nil
}

func mkbucket(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"bucket name"}, MinArgs: 1, RequireBucket: false}); err != nil {
		return err
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
	//TODO --i-know-what-i-do flag to skip questions
	if err := checkArgs(args, argOptions{ArgLabels: []string{"bucket name"}, MinArgs: 1, RequireBucket: false}); err != nil {
		return err
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
/* ###              common helper               ### */
/* ################################################ */

//TODO validate command args with kingpin?
//TODO select specific arg types from list (e.g. bucket name, object name, list type)

type argOptions struct {
	ArgLabels     []string
	MinArgs       int
	RequireBucket bool
}

func checkArgs(args []string, options argOptions) error {
	if options.RequireBucket && len(currentBucket) == 0 {
		return fmt.Errorf("No bucket entered yet. Please list all available buckets via \"list bucket\" and then enter a bucket using \"enter {name}\"")
	}

	if len(args) < options.MinArgs {
		return fmt.Errorf("missing parameter %s", options.ArgLabels[len(args)])
	}
	if len(args) > len(options.ArgLabels) {
		return fmt.Errorf("too many arguments")
	}

	return nil
}

func printObjects(prefix string, filter func(minio.ObjectInfo) bool, nameFormatter func(string) string) error {
	if filter == nil {
		filter = func(minio.ObjectInfo) bool { return true }
	}
	if nameFormatter == nil {
		nameFormatter = func(name string) string { return name }
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

		if filter(obj) {
			list = append(list, obj)
			if !strings.HasSuffix(obj.Key, "/") {
				hasFiles = true
			}
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
				println("  D  %s%s", dirPadding, nameFormatter(obj.Key[len(prefix):len(obj.Key)-1]))
			} else {
				sizeStr := humanize.IBytes(uint64(obj.Size))
				if strings.HasSuffix(sizeStr, " B") {
					// align actual numbers of 1-letter unit 'Byte' with 3-letter units like 'MiB'
					sizeStr = sizeStr + "  "
				}
				padding := strings.Repeat(" ", 11-len(sizeStr))
				println("  F  %s%s  %s", padding, sizeStr, nameFormatter(obj.Key[len(prefix):]))
			}
		}
	}

	return nil
}
