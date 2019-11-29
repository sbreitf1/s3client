package main

import (
	"fmt"
	"strings"

	"github.com/sbreitf1/go-console"
)

var (
	colorWarning   = "\033[1;31m"
	colorHighlight = "\033[1;31m"
	colorTarget    = "\033[1;32m"
	colorPrefix    = "\033[1;34m"
	colorEnd       = "\033[0m"
)

func disableColors() {
	colorWarning = ""
	colorHighlight = ""
	colorTarget = ""
	colorPrefix = ""
	colorEnd = ""
}

func prepareCLE() *console.CommandLineEnvironment {
	cle := console.NewCommandLineEnvironment()
	cle.Prompt = func() string {
		if len(currentBucket) > 0 {
			if len(currentPrefix) > 0 {
				return fmt.Sprintf("%s{%s@%s}%s%s%s", colorTarget, currentBucket, currentTarget.Key, colorPrefix, currentPrefix, colorEnd)
			}
			return fmt.Sprintf("%s{%s@%s}%s", colorTarget, currentBucket, currentTarget.Key, colorEnd)
		}
		return fmt.Sprintf("%s{%s}%s", colorTarget, currentTarget.Key, colorEnd)
	}

	cle.RegisterCommand(console.NewExitCommand("exit"))
	cle.RegisterCommand(console.NewParameterlessCommand("help", help))
	cle.RegisterCommand(console.NewCustomCommand("enter", console.NewFixedArgCompletion(newArgBucket()), enter))
	cle.RegisterCommand(console.NewParameterlessCommand("leave", leave))
	cle.RegisterCommand(console.NewCustomCommand("cd", console.NewFixedArgCompletion(newArgRemoteFile(false)), cd))
	cle.RegisterCommand(console.NewCustomCommand("ls", console.NewFixedArgCompletion(newArgRemoteFile(false)), ls))
	cle.RegisterCommand(console.NewCustomCommand("rm", console.NewFixedArgCompletion(newArgRemoteFile(true)), rm))
	cle.RegisterCommand(console.NewCustomCommand("dl", console.NewFixedArgCompletion(newArgRemoteFile(true), console.NewLocalFileSystemArgCompletion(true)), dl))
	cle.RegisterCommand(console.NewCustomCommand("ul", console.NewFixedArgCompletion(console.NewLocalFileSystemArgCompletion(true), newArgRemoteFile(true)), dl))
	cle.RegisterCommand(console.NewCustomCommand("mv", console.NewFixedArgCompletion(newArgRemoteFile(true), newArgRemoteFile(true)), mv))
	cle.RegisterCommand(console.NewCustomCommand("cp", console.NewFixedArgCompletion(newArgRemoteFile(true), newArgRemoteFile(true)), cp))
	cle.RegisterCommand(console.NewCustomCommand("touch", console.NewFixedArgCompletion(newArgRemoteFile(true)), touch))
	cle.RegisterCommand(console.NewCustomCommand("cat", console.NewFixedArgCompletion(newArgRemoteFile(true)), cat))
	cle.RegisterCommand(console.NewCustomCommand("find", console.NewFixedArgCompletion(nil, newArgRemoteFile(false)), find))
	cle.RegisterCommand(console.NewCustomCommand("list", console.NewFixedArgCompletion(console.NewOneOfArgCompletion("bucket", "env")), list))
	cle.RegisterCommand(console.NewCustomCommand("mkbucket", nil, mkbucket))
	cle.RegisterCommand(console.NewCustomCommand("rmbucket", console.NewFixedArgCompletion(newArgBucket()), rmbucket))

	return cle
}

func runCLE() error {
	cle := prepareCLE()
	return cle.Run()
}

/* ################################################ */
/* ###              arg completion              ### */
/* ################################################ */

type argBucket struct{}

func newArgBucket() *argBucket {
	return &argBucket{}
}

func (a *argBucket) GetCompletionOptions(currentCommand []string, entryIndex int) []console.CompletionOption {
	buckets, err := getBuckets()
	if err == nil {
		return console.PrepareCompletionOptions(buckets, true)
	}
	return nil
}

type argRemoteFile struct {
	withFiles bool
}

func newArgRemoteFile(withFiles bool) *argRemoteFile {
	return &argRemoteFile{withFiles}
}

func (a *argRemoteFile) GetCompletionOptions(currentCommand []string, entryIndex int) []console.CompletionOption {
	files, err := getRemoteFiles(currentPrefix + currentCommand[entryIndex])
	if err == nil {
		candidates := make([]console.CompletionOption, 0)
		for i := range files {
			isDir := strings.HasSuffix(files[i], "/")
			if a.withFiles || isDir {
				parts := strings.Split(files[i], "/")
				label := parts[len(parts)-1]
				if isDir {
					label += parts[len(parts)-2] + "/"
				}
				candidates = append(candidates, console.NewLabelledCompletionOption(label, files[i][len(currentPrefix):], isDir))
			}
		}
		return candidates
	}
	return nil
}

/* ################################################ */
/* ###              read wrappers               ### */
/* ################################################ */

func readln() (string, error) {
	return console.ReadLine()
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

func readpw() (string, error) {
	return console.ReadPassword()
}

func readpwNonEmpty() (string, error) {
	line, err := readpw()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", errUserAbort{}
	}
	return line, nil
}

func println(a ...interface{}) {
	console.Println(a...)
}

func printlnf(format string, args ...interface{}) {
	console.Printlnf(format, args...)
}

type errUserAbort struct{}

func (errUserAbort) Error() string { return "aborted by user" }
