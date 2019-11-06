package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go"
	"github.com/sbreitf1/errors"
	"github.com/sbreitf1/fs"
	"github.com/sbreitf1/fs/path"
)

func help(args []string) error {
	printlnf("Available commands:")
	printlnf("  exit             -  exit application")
	printlnf("  help             -  show this help")
	printlnf("  enter {name}     -  enter bucket with given name")
	printlnf("  leave            -  leave current bucket")
	printlnf("  cd               -  enter named directory or \"..\" for parent dir")
	printlnf("  ls               -  list objects in current bucket and path")
	printlnf("  rm {name}        -  remove object. Use \"-r\" flag to remove all prefixed objects recursively")
	printlnf("  dl {src} {dst}   -  download a remote object {src} and write to local file {dst}")
	printlnf("  ul {src} {dst}   -  upload local file {src} to remote object {dst}")
	printlnf("  mv {src} {dst}   -  copies a remote object {src} to new key {dst} and deletes {src}")
	printlnf("  cp {src} {dst}   -  copies a remote object {src} to new key {dst}")
	printlnf("  touch {name}     -  creates an empty object with key {name}")
	printlnf("  cat {name}       -  print content of object {name}")
	printlnf("  find {needle}    -  list all objects with given {needle} in last part of object key")
	printlnf("  list {type}      -  list items of any type in [bucket, env]")
	printlnf("  mkbucket {name}  -  create new bucket with given name")
	printlnf("  rmbucket {name}  -  delete bucket with given name")
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
		printlnf("No bucket entered yet. Entering bucket %q instead", args[0])
		return enter([]string{args[0]})
	}

	if args[0] == ".." {
		prefix := strings.TrimRight(currentPrefix, "/")
		parts := strings.Split(prefix, "/")
		if len(parts) > 1 {
			currentPrefix = strings.Join(parts[:len(parts)-1], "/") + "/"
		} else {
			currentPrefix = ""
		}
	} else {
		isFile, isDir, _, err := stat(currentPrefix + args[0])
		if err != nil {
			return err
		}
		if isFile {
			return fmt.Errorf("%q is a file", args[0])
		} else if !isDir {
			return fmt.Errorf("Directory %q not found", args[0])
		}

		currentPrefix += args[0] + "/"
	}

	return nil
}

func ls(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"dir name"}, MinArgs: 0, RequireBucket: false}); err != nil {
		return err
	}

	if len(currentBucket) == 0 {
		printlnf("No bucket entered yet. Listing buckets instead")
		return list([]string{"bucket"})
	}

	prefix := currentPrefix
	if len(args) > 0 {
		if strings.HasSuffix(prefix, "/") {
			prefix = args[0]
		} else {
			prefix = args[0] + "/"
		}

		//TODO check existence
	}

	return printObjects(prefix, nil, nil)
}

func rm(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"object name", "arg"}, MinArgs: 1, RequireBucket: true}); err != nil {
		return err
	}

	prefix := currentPrefix + args[0]
	isFile, isDir, _, err := stat(prefix)
	if err != nil {
		return err
	}

	//TODO go back to parent dir if dir is now gone
	if isFile {
		err := minioClient.RemoveObject(currentBucket, prefix)
		if err == nil {
			printlnf("Object %q has been deleted", args[0])
		}
		return err

	} else if isDir {
		if len(args) < 2 || args[1] != "-r" {
			return fmt.Errorf("Please use \"rm {name} -r\" when deleting a directory")
		}

		doneCh := make(chan struct{})
		defer close(doneCh)

		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		// remove all objects with given prefix
		objectCh := minioClient.ListObjectsV2(currentBucket, prefix, true, doneCh)
		for obj := range objectCh {
			if obj.Err != nil {
				return fmt.Errorf("failed to access object: %v", obj.Err)
			}

			if err := minioClient.RemoveObject(currentBucket, obj.Key); err != nil {
				return err
			}

			printlnf("  object %q has been deleted", obj.Key[len(prefix):])
		}

		return nil

	} else {
		return fmt.Errorf("Object %q does not exist", args[0])
	}
}

func dl(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"source", "destination"}, MinArgs: 2, RequireBucket: true}); err != nil {
		return err
	}

	//TODO check object exists

	objKey := currentPrefix + args[0]
	isFile, isDir, _, err := stat(objKey)
	if err != nil {
		return err
	}

	if isFile {
		printlnf("Source Object: %s", objKey)

		len, err := downloadObject(objKey, args[1])
		if err != nil {
			return err
		}

		printlnf("Completed: %s", humanize.IBytes(uint64(len)))
		return nil

	} else if isDir {
		printlnf("Source directory: %s", objKey)

		doneCh := make(chan struct{})
		defer close(doneCh)

		prefix := objKey
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		// find all objects
		list := make([]minio.ObjectInfo, 0)
		objectCh := minioClient.ListObjectsV2(currentBucket, prefix, true, doneCh)
		for obj := range objectCh {
			if obj.Err != nil {
				return fmt.Errorf("failed to access object: %v", obj.Err)
			}

			list = append(list, obj)
		}

		if len(list) == 0 {
			printlnf("Directory is empty")
		} else {
			var totalLen uint64

			localDir := args[1]

			for _, obj := range list {
				localPath := path.Join(localDir, obj.Key[len(prefix):])
				os.MkdirAll(path.Dir(localPath), os.ModePerm)
				printlnf("  downloading file %s", obj.Key[len(prefix):])
				len, err := downloadObject(obj.Key, localPath)
				if err != nil {
					return err
				}

				totalLen += uint64(len)
			}

			if len(list) == 1 {
				printlnf("Completed: %s (%d file)", humanize.IBytes(totalLen), len(list))
			} else {
				printlnf("Completed: %s (%d files)", humanize.IBytes(totalLen), len(list))
			}
		}
		return nil

	} else {

		return fmt.Errorf("Object %q does not exist", args[0])
	}
}

func downloadObject(objKey, filePath string) (int64, error) {
	obj, err := minioClient.GetObject(currentBucket, objKey, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	defer obj.Close()

	f, err := os.Create(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	//TODO download with status bar
	return io.Copy(f, obj)
}

func ul(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"source", "destination"}, MinArgs: 2, RequireBucket: true}); err != nil {
		return err
	}

	//TODO overwrite checks

	localPath := args[0]
	objKey := currentPrefix + args[1]

	if isFile, err := fs.IsFile(localPath); err != nil {
		return err
	} else if isFile {

		printlnf("Upload local file to: %s", objKey)

		len, err := uploadObject(localPath, objKey)
		if err != nil {
			return err
		}

		printlnf("Completed: %s", humanize.IBytes(uint64(len)))
		return nil
	}

	if isDir, err := fs.IsDir(localPath); err != nil {
		return err
	} else if isDir {

		prefix := objKey
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		printlnf("Upload local directory to: %s", objKey)

		localPrefix, _ := path.Abs(localPath)
		if !strings.HasSuffix(localPrefix, "/") {
			localPrefix += "/"
		}

		var totalLen uint64
		if err := fs.Walk(localPath, func(dir string, f fs.FileInfo, isRoot bool) errors.Error {
			localPath, _ := path.Abs(path.Join(dir, f.Name()))
			key := prefix + localPath[len(localPrefix):]

			printlnf("  upload %s to %s", localPath[len(localPrefix):], key)

			len, err := uploadObject(localPath, key)
			if err != nil {
				return errors.Wrap(err)
			}
			totalLen += uint64(len)
			return nil
		}, nil, nil, nil); err != nil {
			return err
		}

		printlnf("Completed: %s", humanize.IBytes(totalLen))
		return nil
	}

	return nil
}

func uploadObject(filePath, objKey string) (int64, error) {
	//TODO upload with status bar
	return minioClient.FPutObject(currentBucket, objKey, filePath, minio.PutObjectOptions{})
}

func mv(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"source", "destination"}, MinArgs: 2, RequireBucket: true}); err != nil {
		return err
	}

	isFile, isDir, _, err := stat(currentPrefix + args[0])
	if err != nil {
		return err
	}

	//TODO check destination

	if isFile {
		src := minio.NewSourceInfo(currentBucket, currentPrefix+args[0], nil)
		dst, err := minio.NewDestinationInfo(currentBucket, currentPrefix+args[1], nil, nil)
		if err != nil {
			return err
		}

		//TODO how to move to parent dir?

		// S3 does not support renaming -> copy and delte old one instead
		if err := minioClient.CopyObject(dst, src); err != nil {
			return fmt.Errorf("Failed to clone object: %s", err.Error())
		}

		if err := minioClient.RemoveObject(currentBucket, currentPrefix+args[0]); err != nil {
			return fmt.Errorf("Unable to delete old object: %s", err.Error())
		}

		printlnf("Object has been moved")
		return nil

	} else if isDir {
		doneCh := make(chan struct{})
		defer close(doneCh)

		prefixSrc := currentPrefix + args[0]
		if !strings.HasSuffix(prefixSrc, "/") {
			prefixSrc += "/"
		}

		prefixDst := currentPrefix + args[1]
		if !strings.HasSuffix(prefixDst, "/") {
			prefixDst += "/"
		}

		// find all objects
		list := make([]minio.ObjectInfo, 0)
		objectCh := minioClient.ListObjectsV2(currentBucket, prefixSrc, true, doneCh)
		for obj := range objectCh {
			if obj.Err != nil {
				return fmt.Errorf("failed to access object: %v", obj.Err)
			}

			list = append(list, obj)
		}

		if len(list) == 0 {
			printlnf("Directory is empty")
		} else {
			var totalLen uint64

			for _, obj := range list {
				printlnf("Move file %q", obj.Key[len(prefixSrc):])

				dstKey := prefixDst + obj.Key[len(prefixSrc):]
				src := minio.NewSourceInfo(currentBucket, obj.Key, nil)
				dst, err := minio.NewDestinationInfo(currentBucket, dstKey, nil, nil)
				if err != nil {
					return err
				}

				if err := minioClient.CopyObject(dst, src); err != nil {
					return fmt.Errorf("failed to copy file %q: %s", obj.Key[len(prefixSrc):], err.Error())
				}

				if err := minioClient.RemoveObject(currentBucket, obj.Key); err != nil {
					return fmt.Errorf("failed to delete previous file %q: %s", obj.Key[len(prefixSrc):], err.Error())
				}

				totalLen += uint64(obj.Size)
			}

			if len(list) == 1 {
				printlnf("Completed: %s (%d file)", humanize.IBytes(totalLen), len(list))
			} else {
				printlnf("Completed: %s (%d files)", humanize.IBytes(totalLen), len(list))
			}
		}
		return nil

	} else {
		return fmt.Errorf("Object %q does not exist", args[0])
	}
}

func cp(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"source", "destination"}, MinArgs: 2, RequireBucket: true}); err != nil {
		return err
	}

	isFile, isDir, _, err := stat(currentPrefix + args[0])
	if err != nil {
		return err
	}

	//TODO check destination

	if isFile {
		src := minio.NewSourceInfo(currentBucket, currentPrefix+args[0], nil)
		dst, err := minio.NewDestinationInfo(currentBucket, currentPrefix+args[1], nil, nil)
		if err != nil {
			return err
		}

		// S3 does not support renaming -> copy and delte old one instead
		if err := minioClient.CopyObject(dst, src); err != nil {
			return fmt.Errorf("Failed to clone object: %s", err.Error())
		}

		printlnf("Object has been copied")
		return nil

	} else if isDir {
		doneCh := make(chan struct{})
		defer close(doneCh)

		prefixSrc := currentPrefix + args[0]
		if !strings.HasSuffix(prefixSrc, "/") {
			prefixSrc += "/"
		}

		prefixDst := currentPrefix + args[1]
		if !strings.HasSuffix(prefixDst, "/") {
			prefixDst += "/"
		}

		// find all objects
		list := make([]minio.ObjectInfo, 0)
		objectCh := minioClient.ListObjectsV2(currentBucket, prefixSrc, true, doneCh)
		for obj := range objectCh {
			if obj.Err != nil {
				return fmt.Errorf("failed to access object: %v", obj.Err)
			}

			list = append(list, obj)
		}

		if len(list) == 0 {
			printlnf("Directory is empty")
		} else {
			var totalLen uint64

			for _, obj := range list {
				printlnf("Copy file %q", obj.Key[len(prefixSrc):])

				dstKey := prefixDst + obj.Key[len(prefixSrc):]
				src := minio.NewSourceInfo(currentBucket, obj.Key, nil)
				dst, err := minio.NewDestinationInfo(currentBucket, dstKey, nil, nil)
				if err != nil {
					return err
				}

				if err := minioClient.CopyObject(dst, src); err != nil {
					return fmt.Errorf("failed to copy file %q: %s", obj.Key[len(prefixSrc):], err.Error())
				}

				totalLen += uint64(obj.Size)
			}

			if len(list) == 1 {
				printlnf("Completed: %s (%d file)", humanize.IBytes(totalLen), len(list))
			} else {
				printlnf("Completed: %s (%d files)", humanize.IBytes(totalLen), len(list))
			}
		}
		return nil

	} else {
		return fmt.Errorf("Object %q does not exist", args[0])
	}
}

func touch(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"object name"}, MinArgs: 1, RequireBucket: true}); err != nil {
		return err
	}

	exists, err := exists(currentPrefix + args[0])
	if err != nil {
		return err
	}
	if exists {
		//TODO update last modified of existing files?
		return fmt.Errorf("Object %q already exists", args[0])
	}

	r := bytes.NewReader([]byte{})
	if _, err := minioClient.PutObject(currentBucket, currentPrefix+args[0], r, 0, minio.PutObjectOptions{}); err != nil {
		return err
	}

	printlnf("Object has been created")
	return nil
}

func cat(args []string) error {
	if err := checkArgs(args, argOptions{ArgLabels: []string{"object name"}, MinArgs: 1, RequireBucket: true}); err != nil {
		return err
	}

	isFile, isDir, _, err := stat(currentPrefix + args[0])
	if err != nil {
		return err
	}
	if isDir {
		return fmt.Errorf("%q is a directory", args[0])
	} else if !isFile {
		return fmt.Errorf("File %q not found", args[0])
	}

	//TODO warn for large files

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

		//TODO check directory exists
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
			printlnf("No buckets found. Use \"mkbucket {name}\" to create one")
		} else {
			if len(buckets) == 1 {
				printlnf("Found 1 bucket:")
			} else {
				printlnf("Found %d buckets:", len(buckets))
			}
			for _, b := range buckets {
				printlnf("  B  %s", b.Name)
			}
		}

	//TODO env

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
	printlnf("bucket %q created", bucketName)
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

	printlnf(colorWarning + "########################################")
	printlnf("###  WARNING: POSSIBLE LOSS OF DATA  ###")
	printlnf("########################################" + colorEnd)
	printlnf("You are about to delete bucket %q.", bucketName)
	printlnf("All data stored in this bucket will be lost and cannot be restored!")
	printlnf("Please confirm deletion by entering the bucket name below:")
	fmt.Print("> ")
	str, err := readln()
	if err != nil {
		return err
	}

	if str != bucketName {
		printlnf("Input mismatch. Bucket was NOT deleted")
		return nil
	}

	printlnf(colorWarning + "#########################################")
	printlnf("###  WARNING: THIS CAN NOT BE UNDONE  ###")
	printlnf("#########################################" + colorEnd)
	printlnf("Are you sure? Please enter DELETE to finally delete the bucket:")
	fmt.Print("> ")
	strDELETE, err := readln()
	if err != nil {
		return err
	}

	if strDELETE != "DELETE" {
		printlnf("Abort. Bucket was NOT deleted")
		return nil
	}

	// delete all objects before deleting the bucket
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := minioClient.ListObjectsV2(bucketName, "", true, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			return fmt.Errorf("failed to access object: %v", obj.Err)
		}

		if err := minioClient.RemoveObject(bucketName, obj.Key); err != nil {
			return fmt.Errorf("failed to delete object %q: %s", obj.Key, err.Error())
		}
	}

	if err := minioClient.RemoveBucket(bucketName); err != nil {
		return err
	}

	printlnf("Bucket %q has been deleted", bucketName)
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

func exists(key string) (bool, error) {
	isFile, isDir, _, err := stat(key)
	if err != nil {
		return false, err
	}
	return (isFile || isDir), nil
}

func isFile(key string) (bool, error) {
	isFile, _, _, err := stat(key)
	if err != nil {
		return false, err
	}
	return isFile, nil
}

func isDir(key string) (bool, error) {
	_, isDir, _, err := stat(key)
	if err != nil {
		return false, err
	}
	return isDir, nil
}

func stat(key string) (isFile bool, isDir bool, fileSize int64, err error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}
	dirKey := key + "/"
	fileKey := key

	objectCh := minioClient.ListObjectsV2(currentBucket, key, false, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			return false, false, 0, fmt.Errorf("failed to access object: %v", obj.Err)
		}

		if obj.Key == dirKey {
			return false, true, 0, nil
		} else if obj.Key == fileKey {
			return true, false, obj.Size, nil
		}
	}

	return false, false, 0, nil
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
		printlnf("No objects found.")
	} else {
		if len(list) == 1 {
			printlnf("Found 1 object:")
		} else {
			printlnf("Found %d objects:", len(list))
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
				printlnf("  D  %s%s", dirPadding, nameFormatter(obj.Key[len(prefix):len(obj.Key)-1]))
			} else {
				sizeStr := humanize.IBytes(uint64(obj.Size))
				if strings.HasSuffix(sizeStr, " B") {
					// align actual numbers of 1-letter unit 'Byte' with 3-letter units like 'MiB'
					sizeStr = sizeStr + "  "
				}
				padding := strings.Repeat(" ", 11-len(sizeStr))
				printlnf("  F  %s%s  %s", padding, sizeStr, nameFormatter(obj.Key[len(prefix):]))
			}
		}
	}

	return nil
}
